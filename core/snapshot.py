"""Versioned, credential-free task snapshots. SHA-256 detects corruption, not authenticity."""
from dataclasses import asdict, fields, is_dataclass
from enum import Enum
import hashlib
import hmac
import json
import math
import re
import time
from typing import Any, Union, get_args, get_origin, get_type_hints

from .contracts import CrawlResponse, CrawlTaskManifest, PipelineError, UnifiedNote
from .persistence import atomic_write_bytes, file_lock
from .security import public_config, redact_error, safe_component, sanitize_url

SCHEMA = 'xhs_task_snapshot_v1'
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
_RESPONSE_FIELDS = ('success', 'driver_name', 'total_notes', 'errors', 'is_partial',
                    'is_synthetic', 'data_source', 'detail_success_count',
                    'detail_failed_count', 'actual_request_count')


class SnapshotError(PipelineError):
    """Invalid, incomplete, unsupported or unreadable task snapshot."""


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SnapshotError('Snapshot contains duplicate JSON keys')
        result[key] = value
    return result


def _reject_constant(_):
    raise SnapshotError('Snapshot contains a nonfinite number')


def _shape(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise SnapshotError('Snapshot fields are missing or unsupported')


def _typed(value, annotation):
    """Decode existing typed contracts without coercing malformed field values."""
    origin, args = get_origin(annotation), get_args(annotation)
    if annotation is Any:
        _canonical(value)
        return value
    if origin is Union:
        for option in args:
            try:
                return _typed(value, option)
            except (SnapshotError, ValueError, TypeError):
                pass
        raise SnapshotError('Snapshot optional field has an invalid type')
    if annotation is type(None):
        if value is not None:
            raise SnapshotError('Snapshot field must be null')
        return None
    if origin is list:
        if not isinstance(value, list):
            raise SnapshotError('Snapshot field must be a list')
        return [_typed(item, args[0]) for item in value]
    if origin is dict:
        if not isinstance(value, dict):
            raise SnapshotError('Snapshot field must be an object')
        return {_typed(k, args[0]): _typed(v, args[1]) for k, v in value.items()}
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if not isinstance(value, str):
            raise SnapshotError('Snapshot enum must be a string')
        return annotation(value)
    if is_dataclass(annotation):
        _shape(value, (item.name for item in fields(annotation)))
        hints = get_type_hints(annotation)
        return annotation(**{key: _typed(item, hints[key]) for key, item in value.items()})
    if annotation is float:
        try:
            finite = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            raise SnapshotError('Snapshot field must be a finite number')
    elif type(value) is not annotation:
        raise SnapshotError('Snapshot field has an invalid type')
    return value


def _public_manifest(data):
    data = dict(data)
    for key in ('effective_config', 'filter_criteria', 'driver_attempts', 'budget_stats'):
        data[key] = public_config(data[key])
    data['errors'] = [redact_error(item) for item in data['errors']]
    return data


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise SnapshotError('Snapshot fingerprint is invalid')


def _count(value):
    if value is not None and (type(value) is not int or value < 0):
        raise SnapshotError('Snapshot count must be a nonnegative integer or null')


def _manifest(data):
    manifest = _typed(data, CrawlTaskManifest)
    safe_component(manifest.task_id)
    for name in ('request_quota', 'actual_discovered_count', 'deduplicated_count',
                 'detail_success_count', 'detail_failed_count', 'actual_request_count'):
        _count(getattr(manifest, name))
    for digest in manifest.content_hashes.values():
        _hash(digest)
    if manifest.config_fingerprint:
        _hash(manifest.config_fingerprint)
    return _typed(_public_manifest(asdict(manifest)), CrawlTaskManifest)


def _note(data):
    _shape(data, [item.name for item in fields(UnifiedNote)] + ['source_hash', 'render_input_hash'])
    values = {key: value for key, value in data.items() if key not in ('source_hash', 'render_input_hash')}
    if any(values[key] is None for key in ('completeness', 'desc_presence', 'media_presence')):
        raise SnapshotError('Snapshot field presence must be explicit')
    note = _typed(values, UnifiedNote)
    safe_component(note.note_id)
    note.image_list = [sanitize_url(url, media=True) for url in note.image_list]
    note.video_url = sanitize_url(note.video_url, media=True)
    for name in ('likes', 'collects', 'comments', 'shares'):
        _count(getattr(note.metrics, name))
    for discovery in note.discovery_records:
        _count(discovery.rank)
    for insight in note.insights:
        if insight.note_id != note.note_id or not 0 <= insight.confidence_score <= 1:
            raise SnapshotError('Snapshot insight does not match its note or confidence range')
    for key, actual in [('source_hash', note.content_hash), ('render_input_hash', note.get_render_input_hash())]:
        _hash(data[key])
        if not hmac.compare_digest(data[key], actual):
            raise SnapshotError('Snapshot note fingerprint does not match its inputs')
    return note


def _decode(payload, allow_synthetic):
    _shape(payload, ['saved_at', 'notes', 'response', 'manifest', 'lineage'])
    _typed(payload['saved_at'], float)
    if not isinstance(payload['notes'], list) or not isinstance(payload['lineage'], list):
        raise SnapshotError('Snapshot notes and lineage must be lists')
    notes = [_note(value) for value in payload['notes']]
    if len({note.note_id for note in notes}) != len(notes):
        raise SnapshotError('Snapshot contains duplicate note identities')
    _shape(payload['response'], _RESPONSE_FIELDS)
    manifest = _manifest(payload['manifest'])
    lineage = [_manifest(item) for item in payload['lineage']]
    previous, identities = None, set()
    for item in [*lineage, manifest]:
        if item.task_id in identities:
            raise SnapshotError('Snapshot task lineage contains a cycle or duplicate identity')
        identities.add(item.task_id)
        if item.operation == 'offline_resume':
            if previous is None or item.parent_task_id != previous.task_id:
                raise SnapshotError('Snapshot offline task has no matching parent')
            _hash(item.source_snapshot_hash)
        elif item.operation in ('crawl', 'jsonl_import'):
            if previous is not None or item.parent_task_id is not None or item.source_snapshot_hash:
                raise SnapshotError('Snapshot capture/import task has inconsistent lineage')
        else:
            raise SnapshotError('Snapshot operation is unsupported')
        previous = item
    hints = get_type_hints(CrawlResponse)
    metadata = {key: _typed(value, hints[key]) for key, value in payload['response'].items()}
    metadata['errors'] = [redact_error(value) for value in metadata['errors']]
    response = CrawlResponse(**metadata, notes=notes, manifest=manifest, snapshot_lineage=lineage)
    for name in ('total_notes', 'detail_success_count', 'detail_failed_count', 'actual_request_count'):
        _count(getattr(response, name))
    if response.total_notes != len(notes) or manifest.deduplicated_count != len(notes):
        raise SnapshotError('Snapshot note statistics do not match its notes')
    if manifest.content_hashes != {note.note_id: note.content_hash for note in notes}:
        raise SnapshotError('Snapshot manifest does not match its content facts')
    sources = [response.data_source, manifest.data_source, *(note.data_source for note in notes)]
    response.is_synthetic = (response.is_synthetic or manifest.is_synthetic or any(n.is_synthetic for n in notes)
                             or any(item.is_synthetic for item in lineage)
                             or any(source in ('mock', 'synthetic_simulation') for source in sources))
    if response.is_synthetic and allow_synthetic is not True:
        raise SnapshotError('Synthetic snapshots require explicit allow_synthetic consent')
    return response


def save_snapshot(response, path):
    """Serialize public task state only; never serialize the whole driver response."""
    try:
        if response.manifest is None:
            raise SnapshotError('A complete task manifest is required for a task snapshot')
        notes = []
        for note in response.notes:
            item = asdict(note)
            item.update(url=sanitize_url(note.url), image_list=[sanitize_url(url, media=True) for url in note.image_list],
                        video_url=sanitize_url(note.video_url, media=True), source_hash=note.content_hash,
                        render_input_hash=note.get_render_input_hash())
            notes.append(item)
        metadata = {key: getattr(response, key) for key in _RESPONSE_FIELDS}
        metadata['errors'] = [redact_error(error) for error in metadata['errors']]
        metadata['is_synthetic'] = response.is_synthetic or any(n.is_synthetic for n in response.notes)
        payload = {'saved_at': time.time(), 'notes': notes, 'response': metadata,
                   'manifest': _public_manifest(asdict(response.manifest)),
                   'lineage': [_public_manifest(asdict(item)) for item in response.snapshot_lineage]}
        _decode(payload, True)  # Validate before touching an existing snapshot.
        canonical = _canonical(payload)
        if len(canonical) > MAX_SNAPSHOT_BYTES:
            raise SnapshotError('Snapshot exceeds the supported size limit')
        document = {'schema': SCHEMA, 'payload': payload, 'payload_sha256': hashlib.sha256(canonical).hexdigest()}
        encoded = (json.dumps(document, ensure_ascii=False, indent=2, allow_nan=False) + '\n').encode('utf-8')
        if len(encoded) > MAX_SNAPSHOT_BYTES:
            raise SnapshotError('Snapshot exceeds the supported size limit')
        with file_lock(str(path) + '.lock'):
            atomic_write_bytes(path, encoded)
        return str(path)
    except SnapshotError:
        raise
    except (ValueError, TypeError, OSError, AttributeError, RecursionError):
        raise SnapshotError('Task snapshot could not be serialized or saved') from None


def load_snapshot(path, *, allow_synthetic=False):
    try:
        with open(path, 'rb') as stream:
            data = stream.read(MAX_SNAPSHOT_BYTES + 1)
        if len(data) > MAX_SNAPSHOT_BYTES:
            raise SnapshotError('Snapshot exceeds the supported size limit')
        document = json.loads(data.decode('utf-8'), object_pairs_hook=_object, parse_constant=_reject_constant)
        _shape(document, ['schema', 'payload', 'payload_sha256'])
        if document['schema'] != SCHEMA:
            raise SnapshotError('Snapshot schema is unsupported; no automatic migration attempted')
        _hash(document['payload_sha256'])
        digest = hashlib.sha256(_canonical(document['payload'])).hexdigest()
        if not hmac.compare_digest(digest, document['payload_sha256']):
            raise SnapshotError('Snapshot checksum does not match its payload')
        response = _decode(document['payload'], allow_synthetic)
        response.snapshot_hash = digest
        return response
    except SnapshotError:
        raise
    except (ValueError, TypeError, OSError, KeyError, AttributeError, RecursionError):
        raise SnapshotError('Task snapshot is unreadable or invalid') from None
