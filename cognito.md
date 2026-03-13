## Basic Auth Elimination

If basic auth is replaced with Cogntio...

Use the following url style: "https://merritt-bucket-bucket-vpc.s3.us-west-2.amazonaws.com/"

### s3-server.rb

```
def protected!
  return true if ENV.fetch('COGNITO', '') = 'Y'
```

### for file urls, use the following...
  dns = "https://#{bucket}.s3.us-west-2.amazonaws.com"



```