import hashlib
from pathlib import Path
from types import SimpleNamespace

import unittest
import tempfile

from remake_video_execution.contracts import SourceSnapshot
from remake_video_execution.references import freeze_pack, frozen_product_paths, resolve_pack, resolve_production_references


class Repo:
    def __init__(self, packs, images):
        self.packs, self.images = packs, images

    def list_where(self, table, where, params):
        if table == 'product_reference_images':
            return [i for i in self.images if i['reference_image_pack_id'] == params[0]]
        return sorted([p for p in self.packs if p['status'] == 'active' and p['product_id'] == params[0]
                       and p['sku_id'] == params[1] and (len(params) == 2 or p['market'] == params[2])],
                      key=lambda p: p['version'], reverse=True)

    def get(self, table, key, value):
        return next((p for p in self.packs if p[key] == value), None)


def fixture():
    packs = [dict(reference_image_pack_id=f'pack{n}', product_id='p', market='TH', sku_id='DEFAULT',
                  version=n, status='active' if n < 3 else 'archived', image_count=3) for n in (1, 2, 3)]
    images = [dict(reference_image_pack_id=p['reference_image_pack_id'], reference_image_id=f"{p['version']}-{i}",
                   image_index=i, image_role='main' if i == 1 else 'detail', object_key=f'{i}.png',
                   file_hash=hashlib.sha256(str(i).encode()).hexdigest()) for p in packs for i in (1, 2, 3)]
    return Repo(packs, images)


class ReferenceTests    (unittest.TestCase):
    def test_latest_active_not_latest_archived(self):
        result = resolve_pack(fixture(), 'p', market='TH')
        assert result['pack']['version'] == 2
        assert len(result['images']) == 3


    def test_explicit_pack_scope_and_missing_images(self):
        repo = fixture()
        assert resolve_pack(repo, 'p', pack_id='pack1')['pack']['version'] == 1
        with self.assertRaisesRegex(ValueError, 'PRODUCT_MISMATCH'):
            resolve_pack(repo, 'other', pack_id='pack1')
        repo.images.pop()
        with self.assertRaisesRegex(ValueError, 'INCOMPLETE'):
            resolve_pack(repo, 'p', pack_id='pack3')


    def test_freezes_all_hashes_preserves_person_and_detects_tampering(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        tmp_path = Path(temp.name)
        class OSS:
            def download(self, key, dest):
                dest.write_bytes(Path(key).stem.encode())
                return SimpleNamespace(success=True)
        source = SourceSnapshot(record_id='r', script_id='s', source_kind='视频复刻', raw_prompt='p', duration_ms=40000,
                                reference_manifest=[{'role': 'PERSON_IDENTITY', 'file_token': 'person'}], source_revision_hash='h')
        result = freeze_pack(source, resolve_pack(fixture(), 'p'), OSS(), tmp_path)
        assert result.reference_image_pack_id == 'pack2'
        assert result.reference_image_version == 2
        assert len(frozen_product_paths(result)) == 3
        assert result.reference_manifest[0]['role'] == 'PERSON_IDENTITY'
        assert result.source_revision_hash != source.source_revision_hash
        # Resume needs no DB/OSS connection and cannot silently move to a newer pack.
        assert resolve_production_references(result, tmp_path) is result
        with self.assertRaisesRegex(ValueError, 'REPLAN_REQUIRED'):
            resolve_production_references(result, tmp_path, pack_id='pack3')
        Path(frozen_product_paths(result)[0]).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'REFERENCE_CHANGED'):
            frozen_product_paths(result)


class UnifiedReferenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.paths = []
        for index in range(3):
            path = self.root / f'{index}.png'
            path.write_bytes(f'image{index}'.encode())
            self.paths.append(str(path))
        self.source = SourceSnapshot(record_id='r', script_id='s', source_kind='视频复刻',
                                     raw_prompt='p', duration_ms=40000, product_id='p', source_revision_hash='old')
        self.fallback_calls = []

    def candidates(self, rows):
        return SimpleNamespace(list_candidates=lambda _: [SimpleNamespace(record_id=r, references=paths) for r, paths in rows])

    def fallback(self, *args, **kwargs):
        self.fallback_calls.append(kwargs)
        return 'AMC'

    def resolve(self, rows, **kwargs):
        return resolve_production_references(self.source, self.root / 'frozen',
            candidate_source=self.candidates(rows), amc_resolver=self.fallback, **kwargs)

    def test_unique_group_and_content_duplicate_select_stably(self):
        result = self.resolve([('b', self.paths), ('a', list(reversed(self.paths)))])
        self.assertEqual(result.reference_selection['group_id'], 'operation:a')
        self.assertEqual(len(frozen_product_paths(result)), 3)
        self.assertFalse(self.fallback_calls)
        self.assertNotEqual(result.source_revision_hash, 'old')
        self.assertTrue(result.reference_selection['content_version_hash'])

    def test_ambiguous_and_invalid_binding_never_fallback(self):
        rows = [('a', self.paths[:1]), ('b', self.paths[1:])]
        with self.assertRaisesRegex(ValueError, 'REFERENCE_GROUP_AMBIGUOUS'):
            self.resolve(rows)
        with self.assertRaisesRegex(ValueError, 'REFERENCE_GROUP_NOT_FOUND'):
            self.resolve(rows, group_id='operation:deleted')
        self.assertFalse(self.fallback_calls)

    def test_binding_wins_over_explicit(self):
        self.source = SourceSnapshot(**{**self.source.to_dict(), 'reference_selection': {'group_id':'operation:b', 'status':'BOUND'}})
        result = self.resolve([('a',self.paths[:1]),('b',self.paths[1:])], group_id='operation:a')
        self.assertEqual(result.reference_selection['group_id'],'operation:b')

    def test_explicit_selects_one_of_multiple(self):
        result = self.resolve([('a',self.paths[:1]),('b',self.paths[1:])], group_id='operation:a')
        self.assertEqual(result.reference_selection['source_record_id'],'a')

    def test_successful_empty_falls_back_but_operation_only_blocks(self):
        self.assertEqual(self.resolve([]), 'AMC')
        with self.assertRaisesRegex(ValueError,'REFERENCE_GROUP_NOT_FOUND'):
            self.resolve([],reference_source='operation')
        self.assertEqual(len(self.fallback_calls),1)

    def test_query_and_download_failures_propagate(self):
        def fail(_):
            raise RuntimeError('FEISHU_FAILURE')
        with self.assertRaisesRegex(RuntimeError,'FEISHU_FAILURE'):
            resolve_production_references(self.source,self.root,candidate_source=SimpleNamespace(list_candidates=fail),amc_resolver=self.fallback)
        with self.assertRaisesRegex(ValueError,'IMAGE_UNAVAILABLE'):
            self.resolve([('a',[str(self.root/'missing.png')])])
        self.assertFalse(self.fallback_calls)

    def test_resume_ignores_latest_but_new_plan_gets_new_content(self):
        frozen = self.resolve([('a',self.paths[:1])])
        Path(self.paths[0]).write_bytes(b'new image')
        resumed = resolve_production_references(frozen,self.root,candidate_source=object(),amc_resolver=self.fallback)
        self.assertIs(resumed,frozen)
        new = self.resolve([('a',self.paths[:1])])
        self.assertNotEqual(new.reference_selection['content_version_hash'],frozen.reference_selection['content_version_hash'])
        self.assertFalse(self.fallback_calls)

    def test_legacy_single_image_resume_does_not_discover(self):
        legacy = SourceSnapshot(**{**self.source.to_dict(),'reference_manifest':[{'role':'PRODUCT_AUTHORITY','path':self.paths[0]}]})
        self.assertIs(resolve_production_references(legacy,self.root,candidate_source=object()),legacy)

    def test_opv_adapter_rejects_partial_download(self):
        from remake_video_execution.reference_sources import operation_source
        source = operation_source()
        source.cache_root = self.root/'cache'
        source._client = SimpleNamespace(download_attachment_bytes=lambda _: (b'', 'bad.png','image/png',0))
        with self.assertRaisesRegex(ValueError,'IMAGES_INCOMPLETE'):
            source._materialize_group('p','r',[{'file_token':'token'}])


class LatestGroupTests(unittest.TestCase):
    def group(self, name, fingerprint, timestamp=None):
        return {'group_id':'operation:'+name,'source_record_id':name,'content_fingerprint':fingerprint,
                'images':[{}], 'record_times':{'last_modified_time':timestamp}}

    def test_latest_system_time_wins_independent_of_scan_order(self):
        from remake_video_execution.reference_sources import select_group
        rows=[self.group('z','old',1700000000000),self.group('a','new',1700000001000),self.group('b','old',1700000000500)]
        for ordered in [rows,list(reversed(rows))]:
            selected=select_group(ordered)
            self.assertEqual(selected['group_id'],'operation:a')
            self.assertEqual(selected['selection_basis']['timestamp_scope'],'RECORD_NOT_ATTACHMENT')

    def test_missing_system_time_does_not_guess(self):
        from remake_video_execution.reference_sources import select_group
        with self.assertRaisesRegex(ValueError,'REFERENCE_GROUP_AMBIGUOUS'):
            select_group([self.group('a','x',1700000000000),self.group('b','y')])

    def test_latest_different_content_tie_blocks_same_content_tie_allowed(self):
        from remake_video_execution.reference_sources import select_group
        with self.assertRaisesRegex(ValueError,'REFERENCE_GROUP_AMBIGUOUS'):
            select_group([self.group('a','x',1700000000000),self.group('b','y',1700000000000)])
        result=select_group([self.group('a','x',1700000000001),self.group('b','x',1700000000001),self.group('c','y',1700000000000)])
        self.assertEqual(result['content_fingerprint'],'x')

    def test_binding_and_explicit_override_newest(self):
        from remake_video_execution.reference_sources import select_group
        rows=[self.group('a','old',1700000000000),self.group('b','new',1700000001000)]
        self.assertEqual(select_group(rows,explicit_group='operation:a')['group_id'],'operation:a')
        self.assertEqual(select_group(rows,bound_group='operation:a',explicit_group='operation:b')['group_id'],'operation:a')
