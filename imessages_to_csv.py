#!/usr/bin/env python3
import sqlite3, csv, time, sys, plistlib, re
from Foundation import (
    NSKeyedUnarchiver,
    NSAttributedString, NSMutableAttributedString,
    NSString, NSMutableString,
    NSDictionary, NSMutableDictionary,
    NSArray, NSMutableArray,
    NSData, NSSet,
    NSNumber, NSURL, NSDate, NSUUID
)

EMOJI_RE = re.compile(r'[\U0001F300-\U0001FAFF\u2600-\u27BF]')

def is_humanish(s: str) -> bool:
    if not s: return False
    s = s.strip()
    if not s: return False
    if s.isdigit(): return False            # drop pure codes/numbers
    if any(ch.isalpha() for ch in s): return True
    if EMOJI_RE.search(s): return True      # allow emoji-only
    return False

def clean_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\r", "\n")
    s = "\n".join(line.strip() for line in s.splitlines())
    # strip obvious object dumps
    if s.startswith("@NSAttributedString"): return ""
    if ("NSAttributedString" in s and s.startswith("<") and s.endswith(">")): return ""
    return s.strip()

def first_string_from(obj):
    """Prefer NSAttributedString.string() or NSString; shallowly search containers."""
    try:
        if obj is None: return ""
        try:
            if obj.isKindOfClass_(NSAttributedString) or obj.isKindOfClass_(NSMutableAttributedString):
                return str(obj.string())
        except Exception: pass
        try:
            if obj.isKindOfClass_(NSString) or obj.isKindOfClass_(NSMutableString):
                return str(obj)
        except Exception: pass
        try:
            if obj.isKindOfClass_(NSDictionary):
                for k in obj.allKeys():
                    s = first_string_from(obj.objectForKey_(k))
                    if is_humanish(s): return s
            if obj.isKindOfClass_(NSArray) or obj.isKindOfClass_(NSMutableArray):
                for i in range(obj.count()):
                    s = first_string_from(obj.objectAtIndex_(i))
                    if is_humanish(s): return s
        except Exception: pass
    except Exception: pass
    return ""

def best_string_from(obj):
    """Collect multiple candidates and return the longest human-ish one."""
    best = ""
    def walk(o):
        nonlocal best
        s = first_string_from(o)
        if is_humanish(s) and len(s) > len(best):
            best = s
        try:
            if getattr(o, 'isKindOfClass_', None):
                if o.isKindOfClass_(NSDictionary):
                    for k in o.allKeys(): walk(o.objectForKey_(k))
                elif o.isKindOfClass_(NSArray) or o.isKindOfClass_(NSMutableArray):
                    for i in range(o.count()): walk(o.objectAtIndex_(i))
        except Exception:
            pass
    walk(obj)
    return clean_text(best)

def decode_attr(blob: bytes) -> str:
    if not blob: return ""
    data = NSData.dataWithBytes_length_(blob, len(blob))
    # Secure unarchive with a generous allowed-class set
    try:
        allowed = NSSet.setWithArray_([
            NSAttributedString, NSMutableAttributedString,
            NSString, NSMutableString,
            NSDictionary, NSMutableDictionary,
            NSArray, NSMutableArray,
            NSData, NSNumber, NSURL, NSDate, NSUUID
        ])
        obj, err = NSKeyedUnarchiver.unarchivedObjectOfClasses_fromData_error_(allowed, data, None)
        if err is None and obj is not None:
            s = best_string_from(obj)
            return s if is_humanish(s) else ""
    except Exception:
        pass
    # Legacy permissive fallback
    try:
        obj, err = NSKeyedUnarchiver.unarchiveTopLevelObjectWithData_error_(data, None)
        if err is None and obj is not None:
            s = best_string_from(obj)
            return s if is_humanish(s) else ""
    except Exception:
        pass
    return ""

def decode_summary(blob: bytes) -> str:
    if not blob: return ""
    try:
        obj = plistlib.loads(blob)
        def pull(x):
            if isinstance(x, str):
                x = clean_text(x)
                return x if is_humanish(x) else ""
            if isinstance(x, bytes):
                try:
                    s = clean_text(x.decode("utf-8","ignore"))
                    return s if is_humanish(s) else ""
                except Exception:
                    return ""
            if isinstance(x, dict):
                for v in x.values():
                    s = pull(v)
                    if s: return s
            if isinstance(x, (list, tuple)):
                for v in x:
                    s = pull(v)
                    if s: return s
            return ""
        s = pull(obj)
        return s if is_humanish(s) else ""
    except Exception:
        return ""

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 imessages_text_only.py <path-to-chat.db> <out.csv>")
        sys.exit(1)

    db_path, out_csv = sys.argv[1], sys.argv[2]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    Q = """
    SELECT
      chat.guid AS chat_guid,
      chat.display_name AS chat_name,
      COALESCE(handle.id, chat.chat_identifier) AS sender_or_chat_id,
      m.is_from_me AS from_me,
      CASE
        WHEN m.date > 1000000000000 THEN (m.date/1000000000)+978307200
        WHEN m.date > 0 THEN m.date+978307200
        ELSE NULL
      END AS ts_unix,
      m.text AS text_plain,
      m.attributedBody AS attr_blob,
      m.message_summary_info AS summary_blob,
      m.associated_message_guid AS assoc_guid,
      m.associated_message_type AS assoc_type,
      m.cache_has_attachments AS has_attach
    FROM message m
    LEFT JOIN handle ON m.handle_id = handle.ROWID
    LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
    LEFT JOIN chat ON chat.ROWID = cmj.chat_id
    ORDER BY m.date ASC;
    """

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["chat_guid","chat_name","sender_or_chat_id",
                    "role","from_me","timestamp_utc",
                    "text_full","source"])

        for r in conn.execute(Q):
            # 1) drop reactions/tapbacks/edits
            if r["assoc_guid"]:
                continue

            # 2) try plain text
            txt = clean_text(r["text_plain"] or "")
            source = "text" if is_humanish(txt) else ""

            # 3) try attributedBody
            if not source:
                t2 = decode_attr(r["attr_blob"])
                if t2:
                    txt, source = t2, "attributedBody"

            # 4) try message_summary_info (link previews, etc.)
            if not source:
                t3 = decode_summary(r["summary_blob"])
                if t3:
                    txt, source = t3, "message_summary_info"

            # 5) final filter: emit ONLY rows with human text
            if not is_humanish(txt):
                continue

            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(r["ts_unix"]))) if r["ts_unix"] else ""
            role = "me" if r["from_me"] == 1 else "other"

            w.writerow([
                r["chat_guid"] or "",
                r["chat_name"] or "",
                r["sender_or_chat_id"] or "",
                role,
                r["from_me"],
                ts,
                txt,
                source
            ])

    conn.close()

if __name__ == "__main__":
    main()
