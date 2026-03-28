import unittest

import numpy as np

from face_matching import (
    build_face_registration_metadata,
    build_student_face_index,
    detect_face_registration_conflict,
    match_face_probe,
)


def make_encoding(base_value=0.0, updates=None):
    arr = np.full(128, float(base_value), dtype=np.float64)
    for index, value in (updates or {}).items():
        arr[int(index)] = float(value)
    return arr


class FaceMatchingTests(unittest.TestCase):
    def test_build_student_face_index_deduplicates_and_keeps_valid_encodings(self):
        primary = make_encoding(0.12)
        near_duplicate = make_encoding(0.12, {0: 0.125})
        distinct = make_encoding(0.12, {4: 0.18})

        face_index = build_student_face_index([
            {
                "student_id": "1001",
                "name": "Student One",
                "encodings": [primary, near_duplicate, distinct, [1, 2, 3]],
            }
        ])

        self.assertEqual(len(face_index["students"]), 1)
        self.assertEqual(face_index["students"][0]["encoding_count"], 2)
        self.assertEqual(face_index["centroids"].shape, (1, 128))

    def test_match_face_probe_returns_best_supported_student(self):
        face_index = build_student_face_index([
            {
                "student_id": "1001",
                "name": "Student One",
                "encodings": [
                    make_encoding(0.11),
                    make_encoding(0.11, {0: 0.13}),
                    make_encoding(0.11, {1: 0.09}),
                ],
            },
            {
                "student_id": "1002",
                "name": "Student Two",
                "encodings": [
                    make_encoding(-0.35),
                    make_encoding(-0.34, {2: -0.30}),
                ],
            },
        ])

        probe = make_encoding(0.11, {0: 0.121, 1: 0.101})
        result = match_face_probe(probe, face_index)

        self.assertTrue(result["recognized"])
        self.assertEqual(result["student"]["student_id"], "1001")
        self.assertEqual(result["reason"], "match")

    def test_match_face_probe_rejects_ambiguous_near_tie(self):
        face_index = build_student_face_index([
            {
                "student_id": "1001",
                "name": "Student One",
                "encodings": [make_encoding(0.0)],
            },
            {
                "student_id": "1002",
                "name": "Student Two",
                "encodings": [make_encoding(0.0, {0: 0.01})],
            },
        ])

        probe = make_encoding(0.0, {0: 0.395})
        result = match_face_probe(probe, face_index)

        self.assertFalse(result["recognized"])
        self.assertEqual(result["reason"], "ambiguous_match")

    def test_detect_face_registration_conflict_flags_other_student_only(self):
        face_index = build_student_face_index([
            {
                "student_id": "1001",
                "name": "Student One",
                "encodings": [
                    make_encoding(0.08),
                    make_encoding(0.08, {0: 0.10}),
                    make_encoding(0.08, {1: 0.06}),
                ],
            },
            {
                "student_id": "1002",
                "name": "Student Two",
                "encodings": [
                    make_encoding(-0.25),
                    make_encoding(-0.25, {2: -0.21}),
                ],
            },
        ])

        new_registration = [
            make_encoding(0.08, {0: 0.095}),
            make_encoding(0.08, {1: 0.07}),
            make_encoding(0.08, {3: 0.085}),
            make_encoding(0.08, {4: 0.09}),
        ]

        self.assertIsNone(
            detect_face_registration_conflict(
                new_registration,
                face_index,
                exclude_student_id="1001",
            )
        )

        conflict = detect_face_registration_conflict(
            new_registration,
            face_index,
            exclude_student_id="9999",
        )
        self.assertIsNotNone(conflict)
        self.assertEqual(conflict["student"]["student_id"], "1001")

    def test_build_face_registration_metadata_summarizes_clean_encodings(self):
        metadata = build_face_registration_metadata([
            make_encoding(0.15),
            make_encoding(0.15, {0: 0.17}),
            make_encoding(0.15, {0: 0.155}),
        ])

        self.assertEqual(metadata["face_encoding_version"], 2)
        self.assertEqual(metadata["face_encoding_count"], 2)
        self.assertEqual(len(metadata["face_encoding_centroid"]), 128)
        self.assertGreaterEqual(len(metadata["face_encoding_hashes"]), 1)


if __name__ == "__main__":
    unittest.main()
