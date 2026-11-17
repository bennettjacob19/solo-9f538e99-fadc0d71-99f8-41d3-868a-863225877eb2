"""test_decoding 解析器单元测试：python3 -m unittest tests.test_decoder -v"""
import unittest

from app.decoder import parse_message

INSERT = (
    "table public.artifacts: INSERT: artifact_no[integer]:1004 "
    "service_name[text]:'cart svc' version[text]:'3.1.0' "
    "sha256[text]:'abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234'"
)
UPDATE = (
    "table public.artifacts: UPDATE: artifact_no[integer]:1001 "
    "service_name[text]:'billing-gateway' version[text]:'1.4.3' "
    "sha256[text]:'1111'"
)
DELETE = "table public.artifacts: DELETE: artifact_no[integer]:1003"
QUOTED = (
    "table public.artifacts: INSERT: artifact_no[integer]:7 "
    "service_name[text]:'it''s a service: x' version[text]:'v 1 2 3' "
    "sha256[text]:'aa'"
)


class DecoderTest(unittest.TestCase):
    def test_begin_commit(self):
        self.assertEqual(parse_message("BEGIN 736", "artifacts"),
                         {"kind": "BEGIN", "xid": 736})
        self.assertEqual(
            parse_message("COMMIT 736 (at 2026-11-16 10:00:00.0+00)",
                          "artifacts"),
            {"kind": "COMMIT", "xid": 736},
        )

    def test_insert(self):
        m = parse_message(INSERT, "artifacts")
        self.assertEqual(m["kind"], "CHANGE")
        self.assertEqual(m["change_type"], "INSERT")
        self.assertEqual(m["artifact_no"], 1004)
        self.assertEqual(m["service_name"], "cart svc")  # 值里有空格
        self.assertEqual(m["version"], "3.1.0")
        self.assertEqual(len(m["sha256"]), 64)

    def test_update_delete(self):
        m = parse_message(UPDATE, "artifacts")
        self.assertEqual(m["change_type"], "UPDATE")
        self.assertEqual(m["artifact_no"], 1001)
        d = parse_message(DELETE, "artifacts")
        self.assertEqual(d["change_type"], "DELETE")
        self.assertIsNone(d.get("version"))

    def test_quoting_and_punctuation(self):
        m = parse_message(QUOTED, "artifacts")
        self.assertEqual(m["service_name"], "it's a service: x")
        self.assertEqual(m["version"], "v 1 2 3")

    def test_ignored_messages(self):
        self.assertIsNone(parse_message("table public.other: INSERT: x[integer]:1",
                                        "artifacts"))
        self.assertIsNone(parse_message("DDL: something", "artifacts"))
        self.assertIsNone(parse_message("sequence public.foo: ...", "artifacts"))


if __name__ == "__main__":
    unittest.main()
