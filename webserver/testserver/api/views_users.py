from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.contrib.auth import authenticate, login, logout
from bin import functions as fn


# ===================================================================
# Users (/user)
# ===================================================================

# /users/all : return a list of all users
def users_all(request):
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()

    try:
        all_users = fn.users_all()
        return JsonResponse(all_users, safe=False)
    except Exception as e:
        return JsonResponse({"result": str(e), "r": -1})


# /users/new?user,password : create a new user
def user_create(request):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    args = fn.args_(request, ["username", "password"], get=False)
    if args is None: return HttpResponseBadRequest()

    try:
        fn.user_create(args["username"], args["password"])
    except Exception as e:
        return JsonResponse({"result": str(e), "r": -1})

    return JsonResponse({"result": "ok", "r": 0})


# /users/<user>/remove
def user_remove(request, user):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    if not fn.access_admin_(request): return HttpResponseForbidden()

    try:
        fn.user_remove(user)
    except Exception as e:
        return JsonResponse({"result": str(e), "r": -1})

    return JsonResponse({"result": "ok", "r": 0})


# /users/<user>/change_password?password
def user_set_password(request, user):
    if request.method != "POST": return HttpResponseBadRequest()
    if not fn.auth_(request): return HttpResponseForbidden()
    args = fn.args_(request, {"new_password": None, "old_password": ""})
    if args is None: return HttpResponseBadRequest()

    if user == "self":
        try:
            if authenticate(username=request.user.username, password=args["old_password"]) is None:
                raise Exception("Wrong password")
            fn.user_set_password(request.user.username, args["new_password"])
        except Exception as e:
            return JsonResponse({"result": str(e), "r": -1})

    else:
        if not fn.access_admin_(request): return HttpResponseForbidden()
        try:
            fn.user_set_password(user, args["new_password"])
        except Exception as e:
            return JsonResponse({"result": str(e), "r": -1})

    return JsonResponse({"result": "ok", "r": 0})
