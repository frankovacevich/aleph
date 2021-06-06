import datetime

from django.http import HttpResponseForbidden, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.utils import timezone
from django.http import Http404

from api.bin import functions as fn


def res_(request, resource_path):
    if not fn.auth_(request): return redirect('/login?after=' + request.path)
    fn.add_activity_log(request)

    res = fn.get_resource_by_uri(resource_path)
    if res is None: raise Http404("Resource not found")
    if res["key"] == "" or fn.access_(request, res["key"]): pass
    else: return HttpResponseForbidden()

    # Extras
    extras = {}
    extras["title"] = res["name"]
    extras["uri"] = resource_path
    for extra in request.GET: extras[extra] = request.GET[extra]
    if "date" not in extras: extras["date"] = datetime.datetime.today().strftime('%Y-%m-%d')
    print(datetime.datetime.now())

    # Files
    try:
        pass
    except:
        raise

    # Templates
    try:
        return render(request, resource_path + ".html", extras)
    except:
        pass

    raise Http404("Resource not found")


def return_xls(filename, wb):
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=' + filename + '.xlsx'
    wb.save(response)
    return response
