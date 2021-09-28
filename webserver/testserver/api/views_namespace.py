import json
from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.contrib.auth import authenticate, login, logout
from bin import functions as fn


# ===================================================================
# Namespace, data
# ===================================================================

# /namespace : returns a list with the namespace paths that the user has access to
def namespace(request):
    if not fn.auth_(request): return HttpResponseForbidden()

    try:
        my_keys = fn.namespace_get(request)
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    return JsonResponse(my_keys, safe=False)


# /namespace/<key>/fields
def namespace_fields(request, key):
    args = fn.check_args_auth_access_(request, {}, key)
    if not isinstance(args, dict): return args

    try:
        fields = fn.namespace_get_fields(key)
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    return JsonResponse(fields, safe=False)


# /namespace/<key>/data?[since],[count],[id],[fields], [utctime]
# {since, count, fields, id, utctime}
def namespace_data(request, key):
    args = fn.check_args_auth_access_(request, {"since": 365, "until": 0, "count": 1, "fields": "*", "id_": "", "utctime": False}, key)
    if not isinstance(args, dict): return args

    if "fields[]" in request.POST:
         args["fields"] = request.POST.getlist("fields[]")
    if "," in args["fields"]: args["fields"] = args["fields"].split(",")

    try:
        if args["id_"] != "":
            data = fn.namespace_retrieve_data_by_id(key, args["id_"])
        else:
            data = fn.namespace_retrieve_data(key, args["fields"], args["since"], args["until"], args["count"])
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    if args["utctime"]:
        for item in data:
            item["t"] = item["t"].strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        from dateutil.tz import tzutc, tzlocal
        for item in data:
            item["t"] = item["t"].replace(tzinfo=tzutc()).astimezone(tzlocal()).strftime("%Y-%m-%d %H:%M:%S")

    return JsonResponse(data, safe=False)


# /namespace/<key>/set_extra
# {field, [alias], [show_on_explorer], [tooltip]}
def namespace_set_extra(request, key):
    if request.method != "POST": return HttpResponseBadRequest()
    args = fn.check_args_auth_access_(request, ["field", "alias", "description"], key, False)
    if not isinstance(args, dict): return args

    a = True
    b = True
    c = True
    try:
        fn.namespace_set_extra(key, args["field"], args["alias"], args["description"])
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    return JsonResponse({"result": "ok", "r": 0})


# /namespace/<key>/publish
# {data:<data>}
def namespace_publish(request, key):
    if request.method != "POST": return HttpResponseBadRequest()
    args = fn.check_args_auth_access_(request, ["data"], key, False)
    if not isinstance(args, dict): return args

    r=0
    try:
        data = json.loads(args["data"])
        for item in data:
            r = fn.publish_data(request.user, key, item)
            if r != 0: break
    except Exception as e:
        return JsonResponse({"result": "Internal Server Error: " + str(e), "r": -1}, status=500)

    if r == 0: return JsonResponse({"result": "ok", "r": r})
    elif r == 1: msg = "MQTT Error: Connection Refused: Unacceptable protocol version"
    elif r == 2: msg = "MQTT Error: Connection Refused: Identifier rejected"
    elif r == 3: msg = "MQTT Error: Connection Refused: Server Unavailable"
    elif r == 4: msg = "MQTT Error: Connection Refused: Bad username or password"
    elif r == 5: msg = "MQTT Error: Connection Refused: Not authorized"
    else: msg = "MQTT Error: Unknown"
    return JsonResponse({"result": msg, "r": r}, status=500)
