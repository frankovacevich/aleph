sudo cp -r testserver/api/* server/api
sudo cp -r testserver/main/* server/main
sudo cp -r testserver/resources/* server/resources
sudo cp -r testserver/static/* server/static
sudo cp -r testserver/templates/* server/templates

cd server
python3 manage.py collectstatic