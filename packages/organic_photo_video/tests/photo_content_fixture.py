"""Explicitly qualified synthetic sources; captions alone never create inventory."""
import copy
import hashlib
from pathlib import Path
from PIL import Image
from config.loader import load_content_recipes

def qualify(repo, root, count=1):
    contract = next(r for r in load_content_recipes() if r.recipe_id == 'PHOTO_TH_PICK_YOUR_LOOK_V3')
    # Keep the older variable schema for selector regression coverage.
    repo.recipe.status = 'active'
    repo.recipe.recipe_id = contract.recipe_id
    spec = repo.recipe.recipe_spec_json
    for key in ('template_id', 'template_version', 'content_card', 'visual_rules', 'outfit_supply'):
        spec[key] = copy.deepcopy(contract.recipe_spec_json[key])
    spec['asset_requirements']['required_roles'] = ['look_a','look_b','look_c','look_d']
    repo.asset_set.manifest_json['assets'] = [a for a in repo.asset_set.manifest_json['assets'] if a['role'] != 'cover_seed']
    sets = []
    for n in range(count):
        aset = copy.deepcopy(repo.asset_set)
        aset.asset_set_id = f'aset-{n+1}'
        aset.asset_set_key = 'TH_WOMENSWEAR_CHOICE' if n == 0 else f'TH_WOMENSWEAR_CHOICE_{n}'
        approval = {'schema_version':'opv-source-qualification-v1','reviewer':'test_fixture','allowed_logic_keys':['short_outerwear_bottom_choice'],'source_hashes':{},'attributes':{}}
        for i,a in enumerate(aset.manifest_json['assets']):
            if n:
                path=Path(root)/f'new-{n}-{i}.jpg';Image.new('RGB',(80,120),(10+n*30, i*50, 70)).save(path)
                a.update(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            a['display_label'] = {
                'th-TH': f'ลุค {chr(65+i)}',
                'zh-CN': f'测试造型 {chr(65+i)}',
            }
            approval['source_hashes'][a['asset_id']]=a['sha256']
            approval['attributes'][a['asset_id']]={'outerwear_id':['coat-a','coat-b','coat-b','coat-d'][i],'bottom_id':f'bottom-{i}'}
        aset.manifest_json['content_approval']=approval
        sets.append(aset)
    for profile in spec['execution_profiles']:
        profile['asset_set_keys']=[a.asset_set_key for a in sets]
    repo.asset_set=sets[0]
    repo.list_asset_sets=lambda **kwargs: list(sets)
    repo.get_asset_set=lambda identity: next((a for a in sets if a.asset_set_id==identity),None)
    return sets
