import json
import unittest

from app.dlq import build_dlq_payload, encode_dlq_payload


class DlqPayloadTests(unittest.TestCase):
    def test_build_dlq_payload_includes_source_and_error_fields(self):
        exc = ValueError("invalid json")

        payload = build_dlq_payload(
            source_topic="transactions_topic",
            source_partition=2,
            source_offset=42,
            source_timestamp=1722435000,
            raw_payload="not-json",
            error=exc,
        )

        self.assertEqual(payload["source_topic"], "transactions_topic")
        self.assertEqual(payload["source_partition"], 2)
        self.assertEqual(payload["source_offset"], 42)
        self.assertEqual(payload["source_timestamp"], 1722435000)
        self.assertEqual(payload["error_type"], "ValueError")
        self.assertEqual(payload["error_message"], "invalid json")
        self.assertEqual(payload["raw_payload"], "not-json")

    def test_encode_dlq_payload_serializes_utf8_json(self):
        payload = {
            "source_topic": "transactions_topic",
            "source_partition": 0,
            "source_offset": 1,
            "source_timestamp": None,
            "error_type": "RuntimeError",
            "error_message": "boom",
            "raw_payload": "{}",
        }

        encoded = encode_dlq_payload(payload)
        decoded = json.loads(encoded.decode("utf-8"))

        self.assertEqual(decoded, payload)


if __name__ == "__main__":
    unittest.main()
