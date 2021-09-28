"""
This is a list with all the resources available to the users.

- Resources can be webapps, static sites, files, etc.
- Resources are identified by a URI.
- Resources are shown on the home screen for each user. (name, group and icon)
- Access to each resource is defined by the namespace 'key' associated.
- Users can pass parameters when requesting a resource (params). Parameters are
  GET arguments.
- When a user access a resource from the home screen, a modal form is displayed
  with a short description and a list of the parameters that can be passed upon
  request.
- When a resource is requested, the callback function is executed. The callback
  function takes the request and a dictionary with the resource params and
  values passed by the user (callback(request, extras)). The callback function
  can return a dict (that is sent as JSON to the user) or a valid HTTP response.
"""

resources = [
    # Example
    # {
    #    "uri": "path/to/resource",
    #    "key": "enterprise.site.area",
    #    "name": "Resource name",
    #    "group": "Group name",
    #    "icon": "icon-from-subtleicons",
    #    "description": "A short description for the user",
    #    "params": {"date": "Insert date (YYYY-MM-DD)"},
    #    "callback": function(request, extras),
    # },
]
