# -*- coding:utf-8 -*-

import json
import logging
import hashlib

from bottle import post, abort, get, route, request

from bomber.db import db
from bomber.plugins import page_plugin
from bomber.utils import get_permission, plain_query
from bomber.models import (
    SystemConfig,
    CallAPIType,
    BomberPtp,
    BomberLog,
    Partner,
    Bomber,
    Role
)
from bomber.serializers import (
    bombers_list_serializer,
    partner_list_serializer,
    role_list_serializer
)
from bomber.validator import (
    edit_bomber_validator,
    new_bomber_validator,
    new_role_validator,
)


# 获取所有的bomber
@get('/api/v1/bombers/list', apply=[page_plugin])
def bombers_list(bomber):
    args = plain_query()
    bombers = (Bomber.select()
               .where(Bomber.is_del == 0)
               .order_by(-Bomber.created_at, Bomber.id))
    if "name" in args:
        bombers = bombers.where(Bomber.name == args['name'])
    if "bid" in args:
        bombers = bombers.where(Bomber.id == args['id'])
    if "role_id" in args:
        bombers = bombers.where(Bomber.role_id == args["role_id"])
    return bombers, bombers_list_serializer


bombers_list.permission = get_permission('System', 'Bombers')


# 添加新的bomber
@post('/api/v1/bombers')
def add_bombers(bomber):
    key_map = {4:"AB_TEST_C1B",5:"AB_TEST_C1B",6:"AB_TEST_C2",8:"AB_TEST_C3"}
    params = new_bomber_validator(request.json)
    name = params.get("username")
    if Bomber.select().where(Bomber.name == name).exists():
        abort(403, 'User already exists')
    password = params.get('password')
    params["password"] = hashlib.md5(password.encode('utf-8')).hexdigest()
    if not params.get('name'):
        params["name"] = name
    if 'ext' in params and not params['ext']:
        params.pop("ext")
    if 'auto_ext' in params and not params['auto_ext']:
        params.pop("auto_ext")
    params["instalment"] = int(params.get("instalment", 0))
    params["group_id"] = int(params.get("group_id", 0))
    new_bomber = None
    role_id = params.get("role")
    if not role_id.isdigit():
        abort(403, "Invalid parameter")
    role_id = int(role_id)
    with db.atomic():
        new_bomber = Bomber.create(**params)
        # 添加到Bomber日志中
        log_params = {
            "bomber_id":new_bomber.id,
            "role_id": role_id,
            "operation":1,
            "operation_id":bomber.id,
            "comment": json.dumps(params)
        }
        BomberLog.create(**log_params)
        sys_key = key_map.get(role_id)
        if not sys_key:
            return bombers_list_serializer.dump(new_bomber).data
        system_config = (SystemConfig.select()
                         .where(SystemConfig.key == sys_key).first())
        if system_config:
            values = json.loads(system_config.value)
            values.append(new_bomber.id)
            system_config.value = json.dumps(values)
            system_config.save()
    if not new_bomber:
        logging.info(
            "add bomber is defeated.bid:%s,params:%s" % (bomber.id, params))
        abort(500, 'Add bomber is defeated')
    return bombers_list_serializer.dump(new_bomber).data


add_bombers.permission = get_permission('System', 'Bombers_Handle')


# 修改bomber数据
@route('/api/v1/bombers/<bomber_id>', method='PATCH')
def edit_bombers(bomber, bomber_id):
    params = edit_bomber_validator(request.json)
    edit_bomber = Bomber.get(Bomber.id == bomber_id)
    if not edit_bomber:
        logging.info(
            "edit bombers,bomber not exists.bid:%s,params:%s,bomber_id:%s" % (
                bomber.id, params, bomber_id))
        abort(403, 'User not exists')
    logging.info(
        "edit bombers.bid:%s,params:%s,bomber_id:%s" % (
        bomber.id, params, bomber_id))
    if not params:
        return bombers_list_serializer.dump(edit_bomber).data
    if params.get("name"):
        if (Bomber.select().where(
                Bomber.name == params.get("name"),
                Bomber.id != bomber_id).exists()):
            abort(403, 'User already exists')
        params["username"] = params["name"]
    if params.get("password"):
        params["password"] = hashlib.md5(
            params.get("password").encode('utf-8')).hexdigest()
    if params.get("partner_id"):
        params["partner_id"] = int(params.get("partner_id"))
    auto_ext = params.get("auto_ext")
    ext = params.get("ext")
    params["auto_ext"] = auto_ext if auto_ext else edit_bomber.auto_ext
    params["ext"] = ext if ext else edit_bomber.ext
    with db.atomic():
        edit_bomber.update_dict(**params)
        edit_bomber.save()
        # 添加到Bomber日志中
        log_params = {
            "bomber_id": bomber_id,
            "role_id": edit_bomber.role_id,
            "operation": 3,
            "operation_id": bomber.id,
            "comment": json.dumps(params)
        }
        BomberLog.create(**log_params)
    return bombers_list_serializer.dump(edit_bomber).data


edit_bombers.permission = get_permission('System', 'Bombers_Handle')


# 删除Bomber
@route('/api/v1/bombers/<bomber_id>', method='DELETE')
def delete_bombers(bomber, bomber_id):
    key_map = {4: "AB_TEST_C1B", 5: "AB_TEST_C1B", 6: "AB_TEST_C2",
               8: "AB_TEST_C3"}
    edit_bomber = Bomber.get(Bomber.id == bomber_id)
    logging.info("delete bombers.bid:%s,bomber_id:%s" % (bomber.id, bomber_id))
    if not edit_bomber:
        return 'ok'
    with db.atomic():
        edit_bomber.is_del = 1
        edit_bomber.password = ''
        edit_bomber.save()
        # 添加到Bomber日志中
        log_params = {
            "bomber_id": bomber_id,
            "role_id": edit_bomber.role_id,
            "operation": 0,
            "operation_id": bomber.id
        }
        BomberLog.create(**log_params)
        # 从systemconfig中移除
        sys_key = key_map.get(edit_bomber.role_id)
        system_config = (SystemConfig.select()
                         .where(SystemConfig.key == sys_key).first())
        if system_config:
            values = json.loads(system_config.value)
            if edit_bomber.id in values:
                values.remove(edit_bomber.id)
                system_config.value = json.dumps(values)
                system_config.save()
        # 删除在bomber_ptp中的记录
        q = (BomberPtp.delete()
             .where(BomberPtp.bomber_id == bomber_id)
             .execute())
    return 'ok'


delete_bombers.permission = get_permission('System', 'Bombers_Handle')


# 获取roles列表
@get('/api/v1/roles')
def roles_list(bomber):
    roles = Role.select().where(Role.id != 0).order_by(Role.created_at,
                                                       Role.id)
    return role_list_serializer.dump(roles, many=True).data


roles_list.permission = get_permission('System', 'Roles')


# 添加roles
@post('/api/v1/roles')
def add_roles(bomber):
    params = new_role_validator(request.json)
    if Role.select().where(Role.name == params.get("name")).exists():
        logging.info("add roles.bid:%s params:%s" % (bomber.id, params))
        abort(403, 'Invalid Role Name')
    params["cycle"] = int(params["cycle"])
    role = Role.create(**params)
    return role_list_serializer.dump(role).data


add_roles.permission = get_permission('System', 'Roles_Handle')


# 获取role的权限
@get('/api/v1/roles/<role_id>/permissions')
def roles_permissions(bomber, role_id):
    role = Role.get(Role.id == role_id)
    if not role:
        abort(403, 'Role not exists')
    return role_list_serializer.dump(role).data


roles_permissions.permission = get_permission('System', 'Roles')


# 修改role的权限
@route('/api/v1/roles/<role_id>/permissions', method='PATCH')
def edit_roles_permissions(bomber, role_id):
    params = request.json
    if not params:
        logging.info(
            "edit roles permissions,Invalid permissions.bid:%s,role_id:%s" % (
                bomber.id, role_id))
        abort(403, 'Invalid permissions')
    role = Role.get(Role.id == role_id)
    if not role:
        logging.info(
            "edit roles permissions,Role not exists.bid:%s,role_id:%s,params:%s" % (
            bomber.id, role_id, params))
        abort(403, 'Role not exists')
    role.permission = json.dumps(params)
    role.save()
    logging.info(
        "edit roles permissions。bid:%s, role_id" % (bomber.id, role_id))
    return 'ok'


edit_roles_permissions.permission = get_permission('System', 'Roles_handle')


# 获取partners列表
@get('/api/v1/partners')
def partners_list(bomber):
    partners = Partner.select().order_by(Partner.created_at, Partner.id)
    return partner_list_serializer.dump(partners, many=True).data


partners_list.permission = get_permission('System', 'Partners')


# 获取bomber中type列表
@get('/api/v1/bombers/types')
def bomber_type_list(bomber):
    type_value = CallAPIType.values()
    return type_value
