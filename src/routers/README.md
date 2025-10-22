# Header with Ink-Username and Ink-Token

## For Admin

Turn on the security access configuration in `config.yml`:

```yaml
common:
  security_access: true

auth:
  type: hai
  issuer: https://aiapi.ihep.ac.cn/apiv2
  client_id: null
  client_secret: <admin key here>
```

## For User

With command:

```bash
curl -X GET /
  -H "Content-Type: application/json" /
  -H "Ink-Username: zhangyiyu@ihep.ac.cn" /
  -H "Ink-Token: <user key here>" /
  http://fastink-test.ihep.ac.cn:8000/api/v1/<routers>
```

## For Developer

When developing routers:

```python
from src.routers.headers import get_username, get_token

@router.get("<routers>")
def router_function(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
):
    data = function(username, token)
    reponse = {"status": str, "msg": str, "data": data}
    return response
```
