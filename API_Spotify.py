# This script is used to test the automation of the access token retrieval.
# This script can be integrated into a flow as a function.

import spotipy
import spotipy.util as util
from spotify_secrets import *

# Demande l'authentification de l'utilisateur
redirect_uri = "http://localhost:8888/callback"
scope = "user-read-recently-played"

token = util.prompt_for_user_token(username, scope, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)

print(token)