from django.db import models
from uuid import uuid4


class ActivityLog(models.Model):
    username = models.CharField(max_length=30)
    date = models.DateTimeField(auto_now_add=True)
    activity = models.CharField(max_length=255)
    ip_address = models.CharField(max_length=40)

    def __str__(self):
        return str(self.date) + " | " + self.username + "@" + self.ip_address + " : " + self.activity


class AccessControl(models.Model):
    username = models.CharField(max_length=30)
    key = models.CharField(max_length=255)
    read_only = models.BooleanField(default=True)

    def __str__(self):
        return self.username + ":" + self.key


class Resource(models.Model):
    uri = models.CharField(max_length=255)
    key = models.CharField(max_length=255, blank=True)
    name = models.CharField(max_length=50, default="Resource name")
    group = models.CharField(max_length=50, default="General")
    icon = models.CharField(max_length=100, default="icon-aleph")
    description = models.CharField(max_length=1000, default="", blank=True)
    params = models.CharField(max_length=1000, default='{"date": "Date (YYYY-MM-DD)"}', blank=True)

    def __str__(self):
        return self.uri + ":" + self.key


class NamespaceExtra(models.Model):
    key = models.CharField(max_length=255)
    field = models.CharField(max_length=255)
    alias = models.CharField(max_length=255, blank=True)
    tooltip = models.CharField(max_length=1000, blank=True)
    show_on_explorer = models.BooleanField(default=True)

    def __str__(self):
        return self.field + " : " + self.alias


class ApiToken(models.Model):
    username = models.CharField(max_length=30)
    token = models.CharField(max_length=40, default=uuid4)
    last_used = models.DateTimeField(auto_now=True)
    # limit = models.IntegerField(default=0)

    def __str__(self):
        return self.username + ":" + self.token
