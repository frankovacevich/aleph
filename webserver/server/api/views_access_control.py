from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.contrib.auth import authenticate, login, logout
from .bin import functions as fn

# ===================================================================
# Access control
# ===================================================================


# /access_control/add
def access_control_add(request):
    fn.add_activity_log(request)
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()
    args = fn.args_(request, ["username", "key"], get=False)
    if args is None: return HttpResponseBadRequest()

    try:
        if not fn.user_exists(args["username"]):
            raise Exception("User does not exist")
        r = fn.access_control_add(args["username"], args["key"])
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    if r is True:
        return JsonResponse({"result": "rule added", "r": 0})
    else:
        return JsonResponse({"result": "rule already exists", "r": 0})


# /access_control/remove?topic,user
def access_control_remove(request):

    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()
    args = fn.args_(request, ["key", "username"], get=False)
    if args is None: return HttpResponseBadRequest()

    try:
        r = fn.access_control_remove(args["username"], args["key"])
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    if r is True:
        return JsonResponse({"result": "rule removed", "r": 0})
    else:
        return JsonResponse({"result": "rule does not exist", "r": 0})


# /access_control/all
def access_control_all(request):

    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()

    try:
        access_control = fn.access_control_all()
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    return JsonResponse({"result": "ok", "r":0, "rules": access_control}, safe=False)
