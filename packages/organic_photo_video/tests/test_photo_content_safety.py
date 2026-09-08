from __future__ import annotations
import copy
import json
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from config.loader import load_board_layouts
from domain.models import ProductionBatch
from domain.photo_contracts import validate_execution_profiles
from services.photo_content import freeze_content_card
from services.photo_package import _compose, normalize_photo_template, PhotoPackageError
from services.photo_request_factory import PhotoRequestFactory, PhotoRequestError
from services.photo_planner import PhotoReusePlannerService
from services.asset_set_service import AssetSetService, AssetSetError
from repositories.rds_repository import RdsRepository, RepositoryError, StaleStatusError
from test_photo_planner import PlannerRepo
from test_rds_repository import FakeConnection
from photo_content_fixture import qualify

class ContentSafetyTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.repo=PlannerRepo(Path(self.tmp.name));qualify(self.repo,Path(self.tmp.name))
        self.spec=self.repo.recipe.recipe_spec_json
        self.card=self.spec['content_card']
        self.layout=next(
            l for l in load_board_layouts()
            if l['layout_id'] == self.spec['template_id']
            and int(l['layout_version']) == int(self.spec['template_version'])
        )
        self.factory=PhotoRequestFactory(self.repo,layouts=[self.layout])
        self.request_spec=SimpleNamespace(recipe_id=self.repo.recipe.recipe_id,account_id='acct',market='TH',language='th-TH')

    def build(self,n=1,overrides=()):
        return self.factory.build_batch(record_id='record',specs=[self.request_spec]*n,category_key='womenswear',overrides=overrides)

    def test_single_rejects_two_sources_before_opening_files(self):
        with self.assertRaisesRegex(PhotoPackageError,'exactly one'):
            _compose(['missing-a','missing-b'],layout='single',width=100,height=100,background='white')

    def test_roles_are_not_page_dependencies_and_cover_has_no_seed(self):
        self.repo.asset_set.manifest_json['assets'].reverse()
        request=self.build()[0]
        plan=PhotoReusePlannerService(self.repo).plan_task(self.repo.task.task_id,recipe_id=request['recipe_id'],variables=request['variables'],copy_block=request['copy'],layout=request['layout_snapshot'],asset_set_id=request['asset_set_id'],recipe_snapshot=request['recipe_snapshot'],asset_snapshot=request['asset_snapshot'],content_card=request['content_card'])['plan']
        self.assertEqual(len(plan['shots']),4)
        self.assertEqual([s['source_slots'] for s in plan['slides']],[[1,2,3,4],[1],[2],[3],[4]])
        self.assertEqual([s['source_refs'] for s in plan['slides']], [['look-2','look-3','look-4','look-5'],['look-2'],['look-3'],['look-4'],['look-5']])
        self.assertEqual(plan['content_card'],request['content_card'])

    def test_changing_bottoms_cannot_be_claimed_as_layering(self):
        card=copy.deepcopy(self.card);card['layering_roles']=['look_a','look_b','look_c']
        for attrs in self.repo.asset_set.manifest_json['content_approval']['attributes'].values():
            attrs['upper_layers']=['inner','coat']
        with self.assertRaisesRegex(ValueError,'changing bottoms'):
            freeze_content_card(card,self.repo.asset_set,{'layering_progression':True})

    def test_relation_and_source_bytes_are_both_binding(self):
        approval=self.repo.asset_set.manifest_json['content_approval']
        visual_rules={'distinct_looks':4,'garment_relations':[
            {'relation':'same_outerwear_different_bottom','roles':['look_b','look_c']}
        ]}
        approval['attributes']['look-4']['outerwear_id']='different-coat'
        with self.assertRaisesRegex(ValueError,'not evidenced'):
            freeze_content_card(self.card,self.repo.asset_set,visual_rules)
        approval['attributes']['look-4']['outerwear_id']='coat-b'
        approval['source_hashes']['look-2']='0'*64
        with self.assertRaisesRegex(ValueError,'hash'):
            freeze_content_card(self.card,self.repo.asset_set,visual_rules)

    def test_one_profile_one_copy_valid_but_same_sources_are_not_two_inventory_items(self):
        self.spec['execution_profiles']=self.spec['execution_profiles'][:1]
        self.spec['execution_profiles'][0]['copy_variants']=self.spec['execution_profiles'][0]['copy_variants'][:1]
        self.assertEqual(validate_execution_profiles(self.spec),[])
        with self.assertRaisesRegex(PhotoRequestError,'请求 2 篇.*仅找到 1'):
            self.build(2)
        self.assertIsNone(self.repo.package)

    def test_source_bound_request_rejects_free_copy_override(self):
        with self.assertRaisesRegex(PhotoRequestError, '不允许自由覆盖文案'):
            self.build(1, [{'copy': {'title': '换标题'}}])

    def test_content_card_descriptions_follow_asset_labels(self):
        self.card['pages'][1]['purpose_zh'] = 'A：{{label_a}}'
        self.repo.asset_set.manifest_json['assets'][0]['display_label'] = {
            'th-TH': 'ลุคหนึ่ง', 'zh-CN': '蓝色外套配长裤',
        }
        frozen = freeze_content_card(self.card, self.repo.asset_set, self.spec['visual_rules'])
        self.assertEqual(frozen['pages'][1]['purpose_zh'], 'A：蓝色外套配长裤')

    def test_existing_batch_signature_is_not_new_inventory(self):
        signature=self.build()[0]['content_card']['content_signature']
        self.repo.list_photo_content_signatures=lambda **kwargs:{signature}
        with self.assertRaisesRegex(PhotoRequestError,'整批未冻结'):
            self.build()

    def test_unknown_nonempty_layout_field_is_rejected(self):
        for payload in ({**self.layout,'safe_area':{'top':220}}, {**self.layout,'render_options':{**self.layout['render_options'],'image_fit':'contain'}}):
            with self.assertRaisesRegex(PhotoPackageError,'unsupported'):
                normalize_photo_template(payload)

    def test_frozen_card_survives_live_recipe_and_asset_status_rollover(self):
        r=self.build()[0]
        self.repo.recipe.status='deprecated';self.repo.recipe.recipe_spec_json={}
        self.repo.asset_set.status='disabled';self.repo.asset_set.manifest_json['content_approval']={}
        result=PhotoReusePlannerService(self.repo).plan_task(self.repo.task.task_id,recipe_id=r['recipe_id'],variables=r['variables'],copy_block=r['copy'],layout=r['layout_snapshot'],asset_set_id=r['asset_set_id'],recipe_snapshot=r['recipe_snapshot'],asset_snapshot=r['asset_snapshot'],content_card=r['content_card'])
        self.assertEqual(result['plan']['content_card'],r['content_card'])

    def test_in_place_source_qualification_edit_requires_new_version(self):
        updated=copy.deepcopy(self.repo.asset_set);updated.manifest_json['content_approval']['reviewer']='changed'
        with self.assertRaisesRegex(AssetSetError,'new id/version'):
            AssetSetService(self.repo).save(updated)

    def test_long_identity_fits_actual_migration_boundaries(self):
        sql=(Path(__file__).resolve().parents[1]/'migrations/002_add_recipe_outfit_package.sql').read_text()
        limits=[int(n) for n in re.findall(r'storyboard_version VARCHAR\((\d+)\)',sql)]
        self.assertEqual(limits,[32,32])
        r=self.build()[0];self.repo.recipe.recipe_id='PHOTO_'+'LONG_ID_'*7
        plan=PhotoReusePlannerService(self.repo).plan_task(self.repo.task.task_id,recipe_id=self.repo.recipe.recipe_id,variables=r['variables'],copy_block=r['copy'],layout=self.layout,asset_set_id=self.repo.asset_set.asset_set_id)['plan']
        # SQLite CHECK explicitly models the MySQL VARCHAR lengths; bare SQLite
        # VARCHAR declarations would not test this production boundary.
        with sqlite3.connect(':memory:') as conn:
            for name,limit in zip(('task','package'),limits):
                conn.execute(f'CREATE TABLE {name}(storyboard_version TEXT CHECK(length(storyboard_version)<={limit}))')
                conn.execute(f'INSERT INTO {name} VALUES (?)',(getattr(self.repo.task if name=='task' else self.repo.package,'storyboard_version'),))
                with self.assertRaises(sqlite3.IntegrityError):conn.execute(f'INSERT INTO {name} VALUES (?)',('x'*33,))
        self.assertEqual(plan['recipe']['id'],self.repo.recipe.recipe_id)

class RepositoryGateTest(unittest.TestCase):
    def batch(self):
        return ProductionBatch(batch_id='b',source_record_id='r',expected_count=1,manifest_json={'media_kind':'native_photo','entries':[{'request':{'schema_version':'opv-photo-request-v2','content_card':{'content_signature':'abc'}}}]})

    def test_cross_batch_reservation_checks_under_lock_and_never_inserts_duplicate(self):
        batch=self.batch()
        conn=FakeConnection([('rows',[{'acquired':1}]),('rows',[]),('rows',[{'manifest_json':json.dumps(batch.manifest_json)}])])
        with self.assertRaisesRegex(RepositoryError,'already frozen'):
            RdsRepository(lambda:conn).create_production_batch_idempotent(batch)
        self.assertEqual(conn.commits,0);self.assertEqual(conn.rollbacks,1)
        self.assertFalse(any('INSERT INTO' in sql for sql,_ in conn.statements))
        self.assertIn('RELEASE_LOCK',conn.statements[-1][0])

    def test_same_batch_retry_returns_original_before_inventory_collision(self):
        batch=self.batch();conn=FakeConnection([('rows',[{'acquired':1}]),('rows',[batch.to_row()])])
        self.assertEqual(RdsRepository(lambda:conn).create_production_batch_idempotent(batch).batch_id,'b')
        self.assertFalse(any('INSERT INTO' in sql for sql,_ in conn.statements))

    def test_cancelled_unreleased_batch_releases_inventory_but_keeps_row(self):
        batch=self.batch()
        conn=FakeConnection([
            ('rows',[batch.to_row()]),
            ('rows',[{'task_id':'task','released_revision_id':None}]),
            ('rows',[]),
            ('rowcount',1),
        ])
        result=RdsRepository(lambda:conn).cancel_photo_batch('r')
        self.assertEqual(result.batch_status,'cancelled')
        self.assertEqual(conn.commits,1)
        self.assertTrue(any("SET batch_status='cancelled'" in sql for sql,_ in conn.statements))

    def test_released_batch_cannot_release_inventory(self):
        batch=self.batch()
        conn=FakeConnection([
            ('rows',[batch.to_row()]),
            ('rows',[{'task_id':'task','released_revision_id':'rev'}]),
        ])
        with self.assertRaisesRegex(RepositoryError,'released'):
            RdsRepository(lambda:conn).cancel_photo_batch('r')
        self.assertEqual(conn.commits,0)
        self.assertEqual(conn.rollbacks,1)

    def test_reusable_outfit_sources_keep_one_photo_per_distinct_look(self):
        rows = [
            {'task_id':'t1','plan_json':json.dumps({'look':{'ref_id':'L1'}}),'slot_index':1,
             'slot_role':'hero','image_path':'/a.jpg','image_sha256':'a'},
            {'task_id':'t1','plan_json':json.dumps({'look':{'ref_id':'L1'}}),'slot_index':2,
             'slot_role':'full_look','image_path':'/b.jpg','image_sha256':'b'},
            {'task_id':'t2','plan_json':json.dumps({'look':{'ref_id':'L1'}}),'slot_index':1,
             'slot_role':'hero','image_path':'/c.jpg','image_sha256':'c'},
            {'task_id':'t3','plan_json':json.dumps({'look':{'ref_id':'L2'}}),'slot_index':1,
             'slot_role':'hero','image_path':'/d.jpg','image_sha256':'d'},
        ]
        conn=FakeConnection([('rows',rows)])
        found=RdsRepository(lambda:conn).list_reusable_outfit_sources('P',limit=4)
        self.assertEqual([(item['task_id'],item['look_ref']) for item in found],[('t1','L1'),('t3','L2')])

    def test_rejection_from_any_prior_revision_blocks_sql_release_transaction(self):
        conn=FakeConnection([('rows',[{'active_revision_id':'new-rev','row_version':3,'media_kind':'native_photo'}]),('rows',[{'review_id':'old-content-rejection'}])])
        with self.assertRaisesRegex(StaleStatusError,'CONTENT_REJECTED'):
            RdsRepository(lambda:conn).release_revision('task','new-rev',expected_task_row_version=3,review_scope='photo_package')
        self.assertEqual(conn.commits,0);self.assertEqual(conn.rollbacks,1)
        self.assertFalse(any(sql.startswith('UPDATE') for sql,_ in conn.statements))
        self.assertIn('WHERE r.task_id=',conn.statements[1][0])
