import hashlib
from typing import Dict, List, Optional, Sequence

import numpy as np


FACE_ENCODING_LENGTH = 128
DEFAULT_SANITIZE_DUPLICATE_DISTANCE = 0.015
DEFAULT_SHORTLIST_SIZE = 8
DEFAULT_MATCH_DISTANCE_THRESHOLD = 0.45
DEFAULT_SCORE_THRESHOLD = 0.47
DEFAULT_SUPPORT_DISTANCE_THRESHOLD = 0.5
DEFAULT_STRONG_MATCH_DISTANCE = 0.36
DEFAULT_MARGIN_THRESHOLD = 0.035
DEFAULT_MIN_SUPPORT_COUNT = 2
DEFAULT_DUPLICATE_DISTANCE_THRESHOLD = 0.30
DEFAULT_DUPLICATE_MEAN_THRESHOLD = 0.36
DEFAULT_DUPLICATE_SUPPORT_DISTANCE = 0.4
DEFAULT_DUPLICATE_SUPPORT_COUNT = 3


def coerce_face_encoding(value) -> Optional[np.ndarray]:
    if isinstance(value, np.ndarray):
        arr = value.astype(np.float64, copy=False).reshape(-1)
    elif isinstance(value, (list, tuple)):
        try:
            arr = np.asarray(value, dtype=np.float64).reshape(-1)
        except (TypeError, ValueError):
            return None
    else:
        return None

    if arr.size != FACE_ENCODING_LENGTH or not np.all(np.isfinite(arr)):
        return None
    return arr


def face_distance(known_encodings, probe_encoding) -> np.ndarray:
    probe = coerce_face_encoding(probe_encoding)
    if probe is None:
        return np.asarray([], dtype=np.float64)

    if isinstance(known_encodings, np.ndarray):
        known = known_encodings.astype(np.float64, copy=False)
        if known.ndim == 1:
            known = known.reshape(1, -1)
    else:
        rows = [coerce_face_encoding(row) for row in list(known_encodings or [])]
        known = np.vstack([row for row in rows if row is not None]) if any(row is not None for row in rows) else np.empty((0, FACE_ENCODING_LENGTH), dtype=np.float64)

    if known.size == 0:
        return np.asarray([], dtype=np.float64)
    return np.linalg.norm(known - probe, axis=1)


def face_distance_to_confidence(distance) -> float:
    try:
        return round(max(0.0, min(100.0, (1.0 - float(distance)) * 100.0)), 2)
    except (TypeError, ValueError):
        return 0.0


def _encoding_signature(encoding: np.ndarray, decimals: int = 5) -> str:
    rounded = np.round(encoding.astype(np.float32), decimals)
    return hashlib.sha1(rounded.tobytes()).hexdigest()


def _normalize_rows(rows) -> List:
    if rows is None:
        return []
    if isinstance(rows, np.ndarray):
        if rows.ndim == 1:
            return [rows]
        return [row for row in rows]
    return list(rows)


def sanitize_face_encodings(
    raw_encodings,
    *,
    max_count: int = 20,
    duplicate_distance: float = DEFAULT_SANITIZE_DUPLICATE_DISTANCE,
) -> List[np.ndarray]:
    sanitized: List[np.ndarray] = []
    for raw in _normalize_rows(raw_encodings):
        encoding = coerce_face_encoding(raw)
        if encoding is None:
            continue
        if sanitized:
            distances = face_distance(sanitized, encoding)
            if len(distances) and float(np.min(distances)) < float(max(duplicate_distance, 0.0)):
                continue
        sanitized.append(encoding)
        if len(sanitized) >= max(int(max_count or 0), 1):
            break
    return sanitized


def build_face_registration_metadata(encodings: Sequence) -> Dict:
    source_rows = _normalize_rows(encodings)
    clean_encodings = sanitize_face_encodings(source_rows, max_count=max(len(source_rows), 1))
    if not clean_encodings:
        return {
            "face_encoding_version": 2,
            "face_encoding_count": 0,
            "face_encoding_centroid": [],
            "face_encoding_hashes": [],
            "face_encoding_spread": 0.0,
        }

    matrix = np.vstack(clean_encodings)
    centroid = np.mean(matrix, axis=0)
    spread = float(np.mean(np.linalg.norm(matrix - centroid, axis=1))) if len(clean_encodings) > 1 else 0.0
    hashes = [_encoding_signature(row) for row in clean_encodings[:10]]
    return {
        "face_encoding_version": 2,
        "face_encoding_count": int(matrix.shape[0]),
        "face_encoding_centroid": centroid.tolist(),
        "face_encoding_hashes": hashes,
        "face_encoding_spread": round(spread, 6),
    }


def build_student_face_index(student_rows: Sequence[Dict], *, max_encodings_per_student: int = 20) -> Dict:
    students = []
    centroids = []

    for row in _normalize_rows(student_rows):
        student_id = str(row.get("student_id") or "").strip()
        name = str(row.get("name") or "").strip()
        if not student_id or not name:
            continue

        encodings = sanitize_face_encodings(
            row.get("encodings", []),
            max_count=max_encodings_per_student,
        )
        if not encodings:
            continue

        matrix = np.vstack(encodings)
        centroid = np.mean(matrix, axis=0)
        centroids.append(centroid)
        students.append({
            "student_id": student_id,
            "name": name,
            "parent_contact": str(row.get("parent_contact") or "").strip(),
            "grade_level": str(row.get("grade_level") or "").strip(),
            "section": str(row.get("section") or "").strip(),
            "student_ref_id": str(row.get("student_ref_id") or "").strip(),
            "encoding_count": int(matrix.shape[0]),
            "encodings": matrix,
            "centroid": centroid,
        })

    return {
        "students": students,
        "centroids": np.vstack(centroids) if centroids else np.empty((0, FACE_ENCODING_LENGTH), dtype=np.float64),
    }


def match_face_probe(
    probe_encoding,
    face_index: Dict,
    *,
    shortlist_size: int = DEFAULT_SHORTLIST_SIZE,
    match_distance_threshold: float = DEFAULT_MATCH_DISTANCE_THRESHOLD,
    score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    support_distance_threshold: float = DEFAULT_SUPPORT_DISTANCE_THRESHOLD,
    strong_match_distance: float = DEFAULT_STRONG_MATCH_DISTANCE,
    margin_threshold: float = DEFAULT_MARGIN_THRESHOLD,
    min_support_count: int = DEFAULT_MIN_SUPPORT_COUNT,
) -> Dict:
    probe = coerce_face_encoding(probe_encoding)
    students = list((face_index or {}).get("students") or [])
    centroids = (face_index or {}).get("centroids")

    if probe is None:
        return {"recognized": False, "reason": "invalid_probe", "candidate": None, "runner_up": None}
    if not students or centroids is None or len(centroids) == 0:
        return {"recognized": False, "reason": "empty_index", "candidate": None, "runner_up": None}

    centroid_distances = face_distance(centroids, probe)
    top_indices = np.argsort(centroid_distances)[: max(int(shortlist_size or 0), 1)]
    candidates = []

    for idx in top_indices:
        student = students[int(idx)]
        sample_distances = face_distance(student["encodings"], probe)
        if len(sample_distances) == 0:
            continue

        ranked = np.sort(sample_distances)
        best_distance = float(ranked[0])
        top_mean_distance = float(np.mean(ranked[: min(len(ranked), 3)]))
        centroid_distance = float(centroid_distances[int(idx)])
        support_count = int(np.count_nonzero(sample_distances <= support_distance_threshold))
        score = float(
            (best_distance * 0.62)
            + (top_mean_distance * 0.23)
            + (centroid_distance * 0.15)
            - (min(support_count, 3) * 0.004)
        )

        candidates.append({
            "student": student,
            "best_distance": best_distance,
            "top_mean_distance": top_mean_distance,
            "centroid_distance": centroid_distance,
            "support_count": support_count,
            "score": score,
            "confidence": face_distance_to_confidence(best_distance),
        })

    if not candidates:
        return {"recognized": False, "reason": "no_candidates", "candidate": None, "runner_up": None}

    candidates.sort(key=lambda row: (row["score"], row["best_distance"], -row["support_count"]))
    best = candidates[0]
    runner_up = candidates[1] if len(candidates) > 1 else None
    strong_match = best["best_distance"] <= strong_match_distance
    required_support = 1 if int(best["student"].get("encoding_count") or 0) <= 1 else max(int(min_support_count or 0), 1)
    score_margin = float(runner_up["score"] - best["score"]) if runner_up else 1.0
    distance_margin = float(runner_up["best_distance"] - best["best_distance"]) if runner_up else 1.0

    rejection_reason = ""
    if best["best_distance"] > match_distance_threshold:
        rejection_reason = "distance_too_high"
    elif best["score"] > score_threshold:
        rejection_reason = "score_too_high"
    elif best["support_count"] < required_support and not strong_match:
        rejection_reason = "insufficient_support"
    elif runner_up and score_margin < margin_threshold and distance_margin < (margin_threshold / 2.0) and not strong_match:
        rejection_reason = "ambiguous_match"

    if rejection_reason:
        return {
            "recognized": False,
            "reason": rejection_reason,
            "candidate": best,
            "runner_up": runner_up,
            "confidence": best["confidence"],
            "distance": best["best_distance"],
            "score_margin": score_margin,
            "distance_margin": distance_margin,
            "candidates": candidates[:3],
        }

    return {
        "recognized": True,
        "reason": "match",
        "student": best["student"],
        "candidate": best,
        "runner_up": runner_up,
        "confidence": best["confidence"],
        "distance": best["best_distance"],
        "score_margin": score_margin,
        "distance_margin": distance_margin,
        "candidates": candidates[:3],
    }


def detect_face_registration_conflict(
    encodings: Sequence,
    face_index: Dict,
    *,
    exclude_student_id: str = "",
    shortlist_size: int = 6,
    duplicate_distance_threshold: float = DEFAULT_DUPLICATE_DISTANCE_THRESHOLD,
    duplicate_mean_threshold: float = DEFAULT_DUPLICATE_MEAN_THRESHOLD,
    support_distance_threshold: float = DEFAULT_DUPLICATE_SUPPORT_DISTANCE,
    min_support_count: int = DEFAULT_DUPLICATE_SUPPORT_COUNT,
) -> Optional[Dict]:
    students = list((face_index or {}).get("students") or [])
    centroids = (face_index or {}).get("centroids")
    source_rows = _normalize_rows(encodings)
    probe_encodings = sanitize_face_encodings(source_rows, max_count=max(len(source_rows), 1))

    if not probe_encodings or not students or centroids is None or len(centroids) == 0:
        return None

    exclude_id = str(exclude_student_id or "").strip()
    probe_matrix = np.vstack(probe_encodings)
    probe_centroid = np.mean(probe_matrix, axis=0)
    centroid_distances = face_distance(centroids, probe_centroid)
    ranked_indices = np.argsort(centroid_distances)
    conflicts = []

    for idx in ranked_indices:
        student = students[int(idx)]
        if exclude_id and str(student.get("student_id") or "").strip() == exclude_id:
            continue
        if len(conflicts) >= max(int(shortlist_size or 0), 1):
            break

        stored_matrix = student["encodings"]
        pairwise = np.linalg.norm(stored_matrix[:, None, :] - probe_matrix[None, :, :], axis=2)
        probe_min = np.min(pairwise, axis=0)
        stored_min = np.min(pairwise, axis=1)
        best_distance = float(np.min(probe_min))
        top_probe_mean = float(np.mean(np.sort(probe_min)[: min(len(probe_min), 3)]))
        top_stored_mean = float(np.mean(np.sort(stored_min)[: min(len(stored_min), 3)]))
        support_count = int(np.count_nonzero(probe_min <= support_distance_threshold))

        if (
            best_distance <= duplicate_distance_threshold
            and top_probe_mean <= duplicate_mean_threshold
            and support_count >= min(max(int(min_support_count or 0), 1), len(probe_min))
        ):
            conflicts.append({
                "student": student,
                "best_distance": best_distance,
                "top_probe_mean": top_probe_mean,
                "top_stored_mean": top_stored_mean,
                "support_count": support_count,
                "centroid_distance": float(centroid_distances[int(idx)]),
            })

    if not conflicts:
        return None

    conflicts.sort(key=lambda row: (row["best_distance"], row["top_probe_mean"], -row["support_count"]))
    return conflicts[0]
