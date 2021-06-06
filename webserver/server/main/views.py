from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.contrib.auth import authenticate, login, logout
from django.utils.safestring import SafeString

from api.bin import functions as fn


# /
def index(request):
	return redirect(home)


# /login
def ulogin(request):
	fn.add_activity_log(request)

	# If session exists: redirect
	if request.user.is_authenticated:
		if "after" in request.GET: return redirect(request.GET["after"])
		return redirect(home)

	if request.method == "GET":
		extra = {}
		if "after" in request.GET:
			extra["after"] = "?after=" + request.GET["after"]
		return render(request, "main/login.html", extra)

	if request.method == "POST":

		username = request.POST["username"]
		password = request.POST["password"]

		user = authenticate(username=username, password=password)

		if user is not None:
			login(request, user)
			if "after" in request.GET: return redirect(request.GET["after"])
			return redirect(home)
		else:
			return render(request, "main/login.html", {"message" : "Invalid username or password"})


# /password_change
def password_change(request):
	if not request.user.is_authenticated: return redirect(ulogin)
	fn.add_activity_log(request)

	if request.method == "GET":
		return render(request, "main/password_change.html")

	if request.method == "POST":
		current_password = request.POST["current_password"]
		new_password = request.POST["new_password"]
		user = authenticate(username=request.user.username, password=current_password)

		if user is None:
			return render(request, "main/password_change.html", {"message": "Invalid password"})
		else:
			fn.user_set_password(request.user.username, new_password)
			return render(request, "main/password_change.html", {"ok_message": "Password changed successfully"})


# /logout
def ulogout(request):
	logout(request)
	return redirect(ulogin)


# /home
def home(request):
	import json
	fn.add_activity_log(request)
	if not request.user.is_authenticated: return redirect(ulogin)

	res = fn.get_resources_available_to_user(request)
	groups = []
	jres = {}
	i = 0
	for r in res:
		jres[i] = r
		r["i"] = i
		i += 1
		if r["group"] not in groups: groups.append(r["group"])

	groups.sort()
	return render(request, "home/index.html", {"res": res, "groups": groups, "jres": json.dumps(jres)})


# /users
def users(request):
	return render(request, "main/users.html", {})


# 404
def not_found(request, exception):
	return render(request, "main/not_found.html")


# 403
def not_allowed(request, exception):
	return render(request, "main/not_allowed.html")


# 400
def bad_request(request, *args, **argv):
	return render(request, "main/bad_request.html")


# 500
def server_error(request, *args, **argv):
	return render(request, "main/server_error.html")


# /explorer
def explorer(request):
	if not fn.auth_(request): return redirect('/login?after=' + request.path)
	fn.add_activity_log(request)
	return render(request, 'explorer/explorer.html', {"title": "Explorer"})
