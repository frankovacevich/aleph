from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.contrib.auth import authenticate, login, logout
from bin import functions as fn


# /namespace/<key>/set_extra
# {field, [alias], [show_on_explorer], [tooltip]}
def namespace_rename_key(request, key):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()
    args = fn.args_(request, ["new_name"], get=False)
    if args is None: return HttpResponseBadRequest()

    try:
        fn.namespace_rename_key(key, args["new_name"])
        return JsonResponse({"result": "ok", "r": 0})
    except Exception as e:
        return JsonResponse({"result": str(e), "r": -1})


def namespace_remove_key(request, key):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()

    try:
        fn.namespace_remove_key(key)
        return JsonResponse({"result": "ok", "r": 0})
    except Exception as e:
        return JsonResponse({"result": str(e), "r": -1})


def namespace_rename_field(request, key, field):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()
    args = fn.args_(request, ["new_name"], get=False)
    if args is None: return HttpResponseBadRequest()

    try:
        fn.namespace_rename_field(key, field, args["new_name"])
        return JsonResponse({"result": "ok", "r": 0})
    except Exception as e:
        return JsonResponse({"result": str(e), "r": -1})


def namespace_remove_field(request, key, field):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()
