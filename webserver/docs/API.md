# API

The REST API allows you to:
- Access namespace metadata (entries, fields) 
- Get data from the database
- Edit the namespace extras
- Edit the access control
- Handle users' accounts (add users, delete users, reset passwords).

```{admonition} NOTE
:class: note
You will need a user account to use the API.
```

```{admonition} NOTE
:class: note
Depending on how the Web Server was set up, you might have some limit on how many API calls you can make. Contact an admin for more information.
```

## Authentication

You will need a **Token** to perform API calls.

```{admonition} NOTE
:class: note
If you are logged in you do not require a token to make API calls. The WebServer provides you with a sessionid cookie that keeps you logged in for some time.
```

```{admonition} NOTE
:class: note
This is the only endpoint that does not require a token. Other API calls require you to authenticate with your token (unless you are logged in and have your sessionid cookie).
```

You can request or recall your token using your username and password and making a call on the following endpoint

```
POST /api/auth
```

**Parameters**
- username (required)
- password (required)

**Reponse**
- 200: If the username and password are valid, it returns a JSON object with the token:
```
{"result": "ok", "r": 0, "token":<token-value>}
```
- 400: The request method is not POST or the username and password were not passed as  parameters.

- 403: The username and password are invalid


## Namespace and Data

### Namespace Entries/Keys

Get all the namespace entries that your user has access to (as defined by the Access Control):
```
POST/GET /api/namespace
```

**Parameters**
- token (required)

**Response**
- 200: A JSON object with a list of all the namespace entries.
```
{"result": "ok", "r": 0, "namespace": ["<entry1>", "<entry2>", ...]}
```
- 403: The authentication failed

- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```

### Namespace Entry's Fields

Get all the fields in a given namespace entry (including all the namespace extra metadata) with any of the following two endpoints (they're equivalent):

```
POST/GET /api/namespace/<key>
```
```
POST/GET /api/namespace/<key>/fields
```

**Arguments**
- key (required): the namespace key/entry

**Parameters**
- token (required)

**Response**
- 200: A JSON object with a list of all the fields in the entry
```
{"result": "ok", "r": 0, "fields"=[
  {
    "field": "<field-name>",
    "tooltip": "<namespace-extra-tooltip>",
    "alias": "<namespace-extra-alias>",
  },
  ...]
}
```
- 403: The authentication failed or the user does not have access to the given key.
- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```

### Data

Get data from a given namespace key:
```
POST/GET /api/namespace/<key>/data
```

**Arguments**
- key (required): the namespace key/entry

**Parameters**
- token (required)
- since (optional, default="365"). Specify the beginning for the timeframe of your query. You can pass an integer number for how many days since the present day you want data, or you can pass a full date as YYYY-MM-DD.
- until (optional, default="0"). Specify the end for the timeframe of your query. You can pass an integer number for how many days since the present day you want data (0 being the present moment), or you can pass a full date as YYYY-MM-DD.
- count (optional, default="1"). Limit the query by how many records you want.
- fields (optional, default="*"). Which fields from the namespace entry you want. You can use "\*" for all the fields, pass a single field as an string, or pass a list of fields (only available for POST requests).
- id_ (optional, default=""). Filter your query by id_ (in case the data you want has an "id_" field).

```{admonition} NOTE
:class: note
If you don't set any parameters and use all the default values you will get the last record of the namespace entry.
```

```{admonition} CAUTION
:class: warning
Using integers for **since** and **until** will get records on a 24 hour basis. 

For example, if you perform the query at noon on a wednesday, using since=2 and until=1, you will get data from monday noon to tuesday noon.
```

```{admonition} CAUTION
:class: warning
Using full dates for **since** and **until** will get records only between those dates, from 00:00:00 on the day _since_ to 23:59:59 on the day _until_.

If you use the same date for both parameters, you will get data from only that date.
```

**Response**
- 200: A JSON object with a list of all the namespace entries.
```
{"result": "ok", "r": 0, "data": {"<field1>": "<value1>", "<field2>": "<value2>", ...]}
```
- 403: The authentication failed or the user does not have access to the given key.
- 500: Unhandled exception:
```
{"result": "<error-message>", "r": -1}
```

### Namespace Extras

To get the extras for a given entry, use the endpoint given above for fields (this will return both fields and the extras).

You can set namspace extras with the following endpoint:

```
POST /api/namespace/<key>/set_extra
```

**Arguments**
- key (required): the namespace key/entry

**Parameters**
- token (required)
- field (required): the field name you want to set extras to.
- alias (optional)
- tooltip (optional)
- show\_on\_explorer (optional): set to _true_ or _false_ to show or hide the field on the explorer.

```{admonition} NOTE
:class: note
You don't have to pass all the parameters, only the ones you want to set for the namespace extras for the field.
```

```{admonition} NOTE
:class: note
A user does not need write permissions to set the namespace extras (write permissions are only for publishing).
```

**Response**
- 200: Ok message
```
{"result": "ok", "r": 0}
```
- 400: The request method is not POST or the _field_ parameter was not passed.
- 403: The authentication failed or the user does not have access to the given key.
- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```

### Publish

Publish data on the namespace with the following endpoint:

```
POST /api/namespace/<key>/publish
```

```{admonition} NOTE
:class: note
The data you publish is relayed by the Web Server to the MQTT Broker as an MQTT message.
```

```{admonition} CAUTION
:class: note
You need write access to publish to the namespace (defined by the Access Control)
```

**Arguments**
- key (required): the namespace key/entry

**Parameters**
- token (required)
- data (required): a JSON string with the data you want to publish ({field: value, ...}).

**Response**
- 200: Ok message
```
{"result": "ok", "r": 0}
```
- 400: The request method is not POST or the _data_ parameter was not passed.
- 403: The authentication failed or the user does not have write access to the given key.
- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```

## Access Control

```{admonition} CAUTION
:class: note
You need an admin account to use any of the endpoints under access control.
```

### Get all rules

Get all the access control rules
```
POST/GET /api/access_control/all
```

**Parameters**
- token (required)

**Response**
- 200: Ok message
```
{"result": "ok", "r": 0, "rules":[{"key": "<key>", "user": "<user>"}, ...] }
```
- 403: The authentication failed or the user is not an admin
- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```

### Add rule


Add a new rule
```
POST /api/access_control/add
```

**Parameters**
- token (required)
- username (required)
- key (required)

**Response**
- 200: If the rule is successfully added, it will return an "ok" message. If the rule already exists, it will return a "rule already exists message.
```
{"result": "rule added", "r": 0}
```
```
{"result": "rule already exists", "r": 0}
```
- 400: The request method was not POST or the username and key parameters where not passed
- 403: The authentication failed or the user is not an admin
- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```


### Remove rule


Remove a new rule
```
POST /api/access_control/remove
```

**Parameters**
- token (required)
- username (required)
- key (required)

**Response**
- 200: Ok message
```
{"result": "rule removed", "r": 0}
```
- 400: The request method was not POST or the username and key parameters where not passed
- 403: The authentication failed or the user is not an admin
- 500: Unhandled exception:
```
{"result": <error-message>, "r": -1}
```


## Users

## Namespace Manager


