# Header with Ink-Username and Ink-Token

## For Admin

Turn on the security access configuration in `config.yml`:

```yaml
common:
  security_access: true

auth:
  type: hai
  issuer: https://your-auth-provider.example.org/api
  client_id: null
  client_secret: <admin key here>
```

## For User

With command:

```bash
curl -X GET /
  -H "Content-Type: application/json" /
  -H "Ink-Username: <username>" /
  -H "Ink-Token: <user key here>" /
  http://<your-host>:8000/api/v2/<routers>
```

## For Developer

When developing routers:

```python
from fastink.routers.headers import get_username, get_token

@router.get("<routers>")
def router_function(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
):
    data = function(username, token)
    reponse = {"status": str, "msg": str, "data": data}
    return response
```
