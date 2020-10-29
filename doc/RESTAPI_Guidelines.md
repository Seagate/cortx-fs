# Rules for JSON (in NFS Management Server) :

**Rule 1 -** keys should be represented by all small letters.
<details>
<summary>Example</summary>
<pre>
{
  name : correct
  message : correct
  ID : wrong
  Address Line : wrong
}
</pre>
</details>

**Rule 2 -** keys should not contain special characters except "_".
<details>
<summary>Example</summary>
<pre>
{
  name : correct
  address_line : correct
  employee-id : wrong
  address#1 : wrong
}
</pre>
</details>

**Rule 3 -** multiword keys should separate words using "_" instead of spaces.
<details>
<summary>Example</summary>
<pre>
{
  address_line : correct
  employee_id : correct
  address line : wrong
  employee id : wrong
}
</pre>
</details>

**Rule 4 -** NULL in c code should not be mapped to null in json.
Absence of a key in json is equivalent to NULL in C code.
<details>
<summary>Example</summary>
<pre>
#wrong
#in this example key address_line exists but its value is undefined.
{
  address_line : null
  employee_id : 1
}

#correct
#in this example key address_line is absent which is treated as NULL.
{
  employee_id : 1
}
</pre>
</details>

**Rule 5 -** arrays or collection of objects should have a plural key.
<details>
<summary>Example</summary>
<pre>
#wrong
{
  error : [1,2,3]
}

#correct 
{
  errors : [1,2,3]
}
</pre>
</details>

**Rule 6 -** keys should contain 'a-z', '0-9' and '_' (Ascii characters) only.
<details>
<summary>Example</summary>
<pre>
{
  address_line_1 : correct
  address_line_2 : correct
  employee_id : correct
  Address~line : wrong
  employee*id : wrong
}
</pre>
</details>

**Rule 7 -** Key names should be meaningful.
<details>
<summary>Example</summary>
<pre>
{
  employee_name : correct
  employee_id : correct
  name : wrong
  id : wrong
}
</pre>
</details>

**Rule 8 -** Reserved words (if any) should be used only for specific cases for
consistency across product.
<details>
<summary>Example</summary>
<pre>
The keys for passing errors should be same are reserved across product. 
They should not be used for passing anything else or have any other meaning.
</pre>
</details>

**Rule 9 -** Avoid naming conflicts by choosing/introducing a new property name
or by versioning the API.
<details>
<summary>Example</summary>
<pre>
assume you have a key "name" and you want to change the contents passes using
the key, its better to either create a new key for that purpose or expose a new
version of the api and modify the key contents in the new version.
</pre>
</details>

# JSON Error Response Schema :

To be consistent in returning errors(when using REST API) with other components
such as S3, we will follow the error format that S3 currently uses 
(Schema shown below).

<details>
<summary>Schema</summary>
<pre>

```
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "error_code": {
      "type": "string"
    },
    "message_id": {
      "type": "string"
    },
    "message": {
      "type": "string"
    },
    "error_format_args": {
      "type": "string"
    }
  },
  "required": [
    "error_code",
    "message"
  ]
}
```

</pre>
</details>

<details>
<summary>Example</summary>
<pre>

```
{
    "error_code": 3,
    "message": "Incorrect Parameters"
}
```

</pre>
</details>

