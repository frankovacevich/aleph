from django.db import models
from uuid import uuid4


class ActivityLog(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    username = models.CharField(max_length=30)
    date = models.DateTimeField(auto_now_add=True)
    activity = models.CharField(max_length=255)
    ip_address = models.CharField(max_length=40)

    def __str__(self):
        return str(self.date) + " | " + self.username + "@" + self.ip_address + " : " + self.activity


class AccessControl(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    username = models.CharField(max_length=30)
    key = models.CharField(max_length=255)
    read_only = models.BooleanField(default=True)

    def __str__(self):
        return self.username + ":" + self.key


class ApiToken(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    username = models.CharField(max_length=30)
    token = models.CharField(max_length=40, default=uuid4)
    last_used = models.DateTimeField(auto_now=True)
    # limit = models.IntegerField(default=0)

    def __str__(self):
        return self.username + ":" + self.token
