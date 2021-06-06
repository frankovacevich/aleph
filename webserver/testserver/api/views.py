from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.contrib.auth import authenticate, login, logout
from .bin import functions as fn


# /auth
# this endpoint takes the username and password and returns the api token
def api_auth(request):
    fn.add_activity_log(request)
    # require post
    if request.method != "POST": return HttpResponseBadRequest()

    # get username and password
    args = fn.args_(request, ["username", "password"], get=False)
    if args is None: return HttpResponseBadRequest()

    user = authenticate(username=args["username"], password=args["password"])
    if user is None: return HttpResponseForbidden()
    login(request, user)

    # get token
    token = fn.token_(username=user.username)
    return JsonResponse({"result": "ok", "r": 0, "token": token})
