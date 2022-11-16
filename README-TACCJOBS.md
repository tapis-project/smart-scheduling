# TACC Job Histories

Part of the smart-scheduling project includes collecting and analysing historical job data.  This discussion describes how ICICLE team members can access to the job execution histories for several TACC HPC systems.  Members of ICICLE that have access to the Tapis *icicle* tenant can perform the actions described here.

The following discussion assumes team members are familiar with issuing command line calls, with *curl*, and with JSON data.  Most Tapis commands return JSON which can be piped to *jq* or a similar program for pretty printing.  The commands below assume **Linux**, if you're on another operating system slight adjustments might be necessary. 

## 1. Get a Tapis Token
Issue either of the following calls to acquire a Tapis JSON Web Token (JWT), replacing YOUR_USERID and YOUR_PASSWORD with valid values.

> curl -X POST -H "Content-Type: application/json" -d '{"username":"YOUR_USERID", "grant_type":"password", "password":"YOUR_PASSWORD"}'  https://icicle.tapis.io/v3/oauth2/tokens

**OR**

> curl -X POST -H "Content-Type: application/json" -d @auth.json https://icicle.tapis.io/v3/oauth2/tokens

where the *auth.json* file contains:
>    {
>       "username": "YOUR_USERID",
>       "password": "YOUR_PASSWORD",
>       "grant_type": "password"
>    }

## 2. Copy Access Token

If the previous step succeeded, the result will look something like this:  

> {"message":"Token created successfully.","metadata":{},"result":{"access_token":{"access_token":"eyJ...y_w","expires_at":"2022-11-16T23:47:11.266665+00:00","expires_in":14400,"jti":"be342fcf-16c7-49cc-a481-ecbda8c0474a"}},"status":"success","version":"dev"}

Copy the value of access token to the clipboard.  The value is inside the double quotes after the 2nd "access_token" key: **eyJ...y_w**.  

## 2. Assign JWT to an Environment Variable

> export JWT=*access_token*

where *access_token* is the text copied to the clipboard in the previous step.

## 3. List Job History Files

> curl -H "X-Tapis-Token: $JWT" https://icicle.tapis.io/v3/files/ops/taccjobs/

## 4. Download a Single History File

> curl -O -H "X-Tapis-Token: $JWT" https://icicle.tapis.io/v3/files/content/taccjobs/maverick_anon.csv.gz

*binary data--do not try to pretty print*

## 5. Download All History Files

> curl -o taccjobs.zip -H "X-Tapis-Token: $JWT" https://icicle.tapis.io/v3/files/content/taccjobs?zip=true

*binary data--do not try to pretty print*
