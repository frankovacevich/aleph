"""

"""
import datetime
import time
import traceback
import random
import json
import sys
import os

aleph_root_folder = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(aleph_root_folder)

from common.bin.logger import Log
from common.bin.namespace_manager import NamespaceManager
from common.lib.custom_mqtt_connection import custom_mqtt_connection
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse

from api.models import ActivityLog
from api.models import AccessControl
from api.models import ApiToken
from resources.resources import resources

NM = NamespaceManager()


def args_(request, args, post=True, get=True):
    """
    gets POST and GET arguments from request and returns a dict
    args can be a list or a dict:
        - if args is a list, the elements on the list are searched on
        the POST and GET parameters, and if one element is not found
        it returns None
        - if args is a dict, each key on the dict is searched on the
        POST and GET paramenters, and if one element is not found, it
        gets its default value as passed on the args dict
    """

    result = {}
    for v in args:
        if isinstance(args, dict) and args[v] is not None:
            result[v] = args[v]
        else:
            if v not in request.GET and v not in request.POST:
                return None

        if v in request.GET and get:
            result[v] = request.GET[v]
        if v in request.POST and post:
            result[v] = request.POST[v]
    return result


def auth_(request):
    """
    Checks if the user is authenticated (has logged in). If it's not, it tries to
    authenticate the user with a token passed as a POST argument.
    If the user is authenticated returns True, else returns False
    """

    if request.user.is_authenticated: return True
    cred = args_(request, ["token"])
    if cred is None: return False
    user = token_(token=cred["token"])
    if user is None: return False

    # OK
    request.user = user

    return True


def access_(request, key, readonly=True):
    """
    Checks if the current user has access to a given key on the namespace
    or resource
    """

    if request.user.is_superuser: return True

    has_access = False
    can_write = False
    for ac in AccessControl.objects.filter(username=request.user.username):
        if ac.key == "#":  has_access = True
        if key == ac.key:  has_access = True
        if key.startswith(ac.key.replace(".#", "")): has_access = True

        if has_access and not ac.read_only:
            can_write = True
            break

    if not has_access: return False
    if readonly: return True
    if can_write: return True
    return False


def token_(username=None, token=None):
    """
    Returns the username for the given token, or the token for the given username
    If the username parameter is used and the token does not exist, creates and returns a new token
    If the token parameter is used and the username does not exist, returns None
    """
    if username is not None:
        t = ApiToken.objects.filter(username=username)
        if t:
            return t[0].token
        else:
            ApiToken.objects.create(username=username)
            return token_(username=username)

    elif token is not None:
        t = ApiToken.objects.filter(token=token)
        if t:
            return User.objects.get(username=t[0].username)
        else:
            return None
    return None


def access_admin_(request):
    """
    Checks if the user is superuser
    """
    return request.user.is_authenticated and request.user.is_staff


def check_args_auth_access_(request, args, key, readonly=True):
    """
    Performs three checks:
    - Checks if the current user is authenticated (If not returns Forbidden)
    - Checks if the current user has access to the given key on the namespace
      (If not returns Forbidden)
    - Checks if the request has all required arguments (POST or GET)
      (If not returns Bad Request)

    If all checks are okay, returns a dict of arguments
    """

    # check auth
    if not auth_(request):
        return HttpResponseForbidden()

    # check access control
    if not access_(request, key, readonly):
        return HttpResponseForbidden()

    # check args
    this_args = args_(request, args)
    if this_args is None:
        return HttpResponseBadRequest()

    return this_args


# ===================================================================
# Activity log
# ===================================================================
def add_activity_log(request):
    # Get IP
    x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    if x_forwarded_for:
        ip = x_forwarded_for.split(",")[0]
    else:
        ip = request.META.get("REMOTE_ADDR")

    username = request.user.username
    date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    activity = request.build_absolute_uri()
    ActivityLog.objects.create(username=username, date=date, activity=activity, ip_address=ip)


# ===================================================================
# Resources
# ===================================================================
def get_resources_available_to_user(request):
    allowed_resources = []
    for r in resources:
        if r["key"] == "" or access_(request, r["key"]):
            allowed_resources.append({f: r[f] for f in r if f != "callback"})
    return allowed_resources


def get_resource_by_uri(uri):
    r = [x for x in resources if x["uri"] == uri]
    if len(r) == 0: return None
    return r[0]


# ===================================================================
# Users
# ===================================================================
def user_exists(username):
    u = User.objects.filter(username=username)
    if u: return True
    return False


def users_all():
    return [{"username": u.username, "is_admin": u.is_superuser} for u in User.objects.all()]


def user_set_password(username, new_password='1234'):
    u = User.objects.get(username=username)
    if u:
        u.set_password(new_password)
        u.save()
        return
    raise Exception("User doesn't exists")


def user_remove(username):
    u = User.objects.get(username=username)
    if u:
        u.delete()
        return
    raise Exception("User doesn't exists")


def user_create(username, password):
    User.objects.create_user(username=username, password=password)
    return


# ===================================================================
# Access control
# ===================================================================

# returns a list with all access control records
def access_control_all():
    return [{"key":ac.key, "user": ac.username} for ac in AccessControl.objects.all()]


# returns a list with all the keys to which the user has access
def access_control_by_user(user):
    return [ac.key for ac in AccessControl.objects.all() if ac.username == user]


# creates an access control record
def access_control_add(user, key):
    existing_rules = access_control_by_user(user)
    if key in existing_rules:
        return False
    AccessControl.objects.create(username=user, key=key)
    return True


# removes access control record
def access_control_remove(user, key):
    ac = AccessControl.objects.filter(username=user, key=key)
    if ac:
        ac.delete()
        return True
    return False


# ===================================================================
# Namespace
# ===================================================================

def namespace_get(request):
    NM.connect()
    namespace = NM.get_keys()
    NM.close()
    if namespace is None: return []
    allowed_namespace = []

    for key in namespace:
        if access_(request, key):
            allowed_namespace.append(key)

    return allowed_namespace


def namespace_get_fields(key):
    NM.connect()
    fields = NM.get_metadata(key)
    NM.close()
    return fields


def namespace_retrieve_data(key, fields, since, until, count):
    NM.connect()
    all_data = []

    if isinstance(count, str): count = int(count)
    if isinstance(since, str) and since.isnumeric(): since = int(since)
    if isinstance(until, str) and until.isnumeric(): until = int(until)

    for field in fields:
        x = NM.get_data(key, field, since, until, count)
        all_data += x
    NM.close()

    return all_data


def namespace_retrieve_data_by_id(key, id_):
    NM.connect()
    dat = NM.get_data_by_id(key, id_)
    NM.close()
    return dat


def namespace_set_extra(key, field, alias, description):
    NM.connect()
    print(key, field, alias, description)
    NM.set_metadata(key, field, alias, description)
    NM.close()


# ===================================================================
# MQTT
# ===================================================================

def publish_data(user, key, data):
    mqtt_connection = custom_mqtt_connection(user.username + str(random.randint(0, 9999)))
    mqtt_connection.connect()
    while not mqtt_connection.connected:
        mqtt_connection.loop()

    r = mqtt_connection.publish(data, key.replace(".", "/"))
    mqtt_connection.disconnect()
    return r


# ===================================================================
# FUNCTIONS NAMESPACE MANAGER
# ===================================================================

def namespace_rename_key(key, new_name):
    NM.connect()
    NM.rename_key(key, new_name)
    NM.close()
    return


def namespace_remove_key(key):
    NM.connect()
    NM.remove_key(key)
    NM.close()
    return


def namespace_rename_field(key, field, new_name):
    NM.connect()
    NM.rename_field(key, field, new_name)
    NM.close()
    return


def namespace_remove_field(key, field):
    NM.connect()
    NM.remove_field(key, field)
    NM.close()
    return
