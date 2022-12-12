import requests
from spotify_secrets import *

auth_url = "https://accounts.spotify.com/api/token"

auth_header = {}
auth_data = {}

client_ID = "69f63605c9c54eaba5671119d368fb23"
client_secret = "17219521d8e1491fb477e3798c7014b4"

# Base64 encode Client ID and Client Secret 