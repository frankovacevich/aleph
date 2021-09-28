import datetime

from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import redirect
from django.http import Http404

from bin import functions as fn


def res_(request, resource_path):

    # 1. Check if user is logged in
    if not fn.auth_(request): return redirect('/login?after=' + request.path)
    fn.add_activity_log(request)

    # 2. See if resource exists. If it doesn't, return 404
    res = fn.get_resource_by_uri(resource_path)
    if res is None: raise Http404("Resource not found")
    if not fn.access_(request, res["key"]): return HttpResponseForbidden()

    # 3. Get extras from GET
    extras = {"res": res, "date_now": datetime.datetime.today().strftime('%Y-%m-%d')}
    for extra in request.GET: extras[extra] = request.GET[extra]

    # 4. Get response from callback function
    callback = res["callback"]
    response = callback(request, extras)

    if isinstance(response, dict) or isinstance(response, list):
        return JsonResponse({"result": "ok", "r": 0, "data": response}, safe=False)
    else:
        return response
