# This script is used to test the automation of the access token retrieval.
# This script can be integrated into a flow as a function.

import spotipy
import spotipy.util as util
from spotify_secrets import *

# Storing a dummy uri and the data's scope
redirect_uri = "http://localhost:8888/callback"
scope = "user-read-recently-played"

# Spotify's authentification, gives our token if successful
token = util.prompt_for_user_token(username, scope, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)

print(token)