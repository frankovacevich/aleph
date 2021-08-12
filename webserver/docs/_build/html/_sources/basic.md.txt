# About the Web Server

This is the Web Server documentation. This documentation can be edited to suit your application's usage.

The Web Server is the interface between users and the data. From the web interface, users can view the data in real time, query the databases, visualize the data with quick or complex views, generate reports for printing or downloading, modify and create new information, etc.

## The Namespace

Aleph structures data within a namespace. The namespace is a hierarchical structure similar to folders in a file system. The namespace contains keys or entries identified by a name that must not contain capital letters or spaces. The levels of the namespace are separated by dots (.).

Each key of the namespace contains data (records) consisting of multiple fields (columns).

The namespace also stores metadata about each field in each key, like an alias name and a brief description.

## Users

There are three types of users:

- Admins, that can access the admin console and edit other user's info
- Superusers, that have access to all the resources in the web server
- Normal users

In case a superuser or normal user loses their password, they should contact an admin and request a password reset.

## Access control

The way resources are accessed on the web server is tied together to the namespace structure. Admins can set the rules to allow different users to view and modify different namespace keys. At the same time, each resource is associated with a namespace key. This way, users have access to different data and resources within the WebServer.
