import requests
from spotify_secrets import *

auth_url = "https://accounts.spotify.com/api/token"

auth_header = {}
auth_data = {}

client_ID = ""
client_secret = ""

# Base64 encode Client ID and Client Secret 