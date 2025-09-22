# Converting API responses

## Introduction

The NMDC Runtime API returns NMDC metadata in JSON format.

One thing we've learned from talking with our users is that some of them prefer working with metadata in CSV format.

In this how-to guide, we'll show you how you can convert JSON-formatted Runtime API responses into various formats, including CSV.

## Converting JSON into CSV

Here's how you can convert JSON into CSV.

> **Note:** In this section, we'll be demonstrating a process that produces a CSV string in which each column name is a **[JSONPath](https://en.wikipedia.org/wiki/JSONPath) expression** that indicates precisely where—in the original JSON object(s)—the value(s) in that column came from.
>
> For example, the column name "`$.principal_investigator.has_raw_value`" indicates that the value(s) in that column came from the "`has_raw_value`" field of the object in the "`principal_investigator`" field of the root object (represented by "`$`").

### Prerequisites

1. The [jq](https://jqlang.org/) CLI application is installed
   > You can check whether it's installed by running `$ jq --version`. You can install it by following the instructions on its website.
2. The [`json-tabulate`](https://pypi.org/project/json-tabulate/) Python package is installed
   > You can check whether it's installed by running `$ json-tabulate --version`. You can install it by running `$ pip install json-tabulate`.

### Procedure

Here's how you can convert the JSON-formatted response from an NMDC Runtime API endpoint into CSV.

```shell
curl --silent 'https://api.microbiomedata.org/studies?fields=id,name,principal_investigator.has_raw_value&per_page=3' \
  | jq '.results' \
  | json-tabulate
```

We'll break it down here.

The first command—`curl ...`—sends an HTTP request to the NMDC Runtime API and outputs the JSON-formatted response, which looks like this:

```json
{
  "meta": {
    "mongo_filter_dict": {},
    "mongo_sort_list": null,
    "count": 47,
    "db_response_time_ms": 0,
    "page": 1,
    "per_page": 3,
    "fields": "id,name,principal_investigator.has_raw_value"
  },
  "results": [
    {
      "id": "nmdc:sty-11-8fb6t785",
      "name": "Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia, USA",
      "principal_investigator": {
        "has_raw_value": "Kelly Wrighton"
      }
    },
    {
      "id": "nmdc:sty-11-33fbta56",
      "name": "Peatland microbial communities from Minnesota, USA, analyzing carbon cycling and trace gas fluxes",
      "principal_investigator": {
        "has_raw_value": "Christopher Schadt"
      }
    },
    {
      "id": "nmdc:sty-11-aygzgv51",
      "name": "Riverbed sediment microbial communities from the Columbia River, Washington, USA",
      "principal_investigator": {
        "has_raw_value": "James Stegen"
      }
    }
  ],
  "group_by": []
}
```

We wanted a list of studies, but the API response included some metadata about pagination and we don't want to analyze that. The list of studies we wanted is nested within a property named `results`.

We can pipe the output of the `curl ...` command into `jq` in order to isolate the `results` list, like this:

```shell
curl --silent 'https://api.microbiomedata.org/studies?fields=id,name,principal_investigator.has_raw_value&per_page=3' \
  | jq '.results'
```

The output is now:

```json
[
  {
    "id": "nmdc:sty-11-8fb6t785",
    "name": "Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia, USA",
    "principal_investigator": {
      "has_raw_value": "Kelly Wrighton"
    }
  },
  {
    "id": "nmdc:sty-11-33fbta56",
    "name": "Peatland microbial communities from Minnesota, USA, analyzing carbon cycling and trace gas fluxes",
    "principal_investigator": {
      "has_raw_value": "Christopher Schadt"
    }
  },
  {
    "id": "nmdc:sty-11-aygzgv51",
    "name": "Riverbed sediment microbial communities from the Columbia River, Washington, USA",
    "principal_investigator": {
      "has_raw_value": "James Stegen"
    }
  }
]
```

Much better! Lastly, this how-to guide is about converting JSON-formatted metadata into other formats; in this case, into CSV.

We can pipe the output of the `jq ...` command into `json-tabulate` in order to convert the metadata into CSV, like this:

```shell
curl --silent 'https://api.microbiomedata.org/studies?fields=id,name,principal_investigator.has_raw_value&per_page=3' \
  | jq '.results' \
  | json-tabulate
```

The output is now:

```csv
$.id,$.name,$.principal_investigator.has_raw_value
nmdc:sty-11-8fb6t785,"Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia, USA",Kelly Wrighton
nmdc:sty-11-33fbta56,"Peatland microbial communities from Minnesota, USA, analyzing carbon cycling and trace gas fluxes",Christopher Schadt
nmdc:sty-11-aygzgv51,"Riverbed sediment microbial communities from the Columbia River, Washington, USA",James Stegen
```

Or, as a table:

|$.id                |$.name                                                                                           |$.principal_investigator.has_raw_value|
|--------------------|-------------------------------------------------------------------------------------------------|--------------------------------------|
|nmdc:sty-11-8fb6t785|Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia, USA    |Kelly Wrighton                        |
|nmdc:sty-11-33fbta56|Peatland microbial communities from Minnesota, USA, analyzing carbon cycling and trace gas fluxes|Christopher Schadt                    |
|nmdc:sty-11-aygzgv51|Riverbed sediment microbial communities from the Columbia River, Washington, USA                 |James Stegen                          |

> **Wondering why all the column names begin with `$`?**
>
> The column names are [JSONPath](https://en.wikipedia.org/wiki/JSONPath) expressions. They indicate precisely where—in the original JSON object(s)—the value(s) in that column came from.

## Converting JSON into TSV

You can convert JSON into TSV by following the same procedure as converting JSON into CSV (see above); except that, for TSV, the `json-tabulate` command would include the `--output-format tsv` option, like this:

```shell
curl --silent 'https://api.microbiomedata.org/studies?fields=id,name,principal_investigator.has_raw_value&per_page=3' \
  | jq '.results' \
  | json-tabulate --output-format tsv
```

The output would be:

```tsv
$.id	$.name	$.principal_investigator.has_raw_value
nmdc:sty-11-8fb6t785	Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia, USA	Kelly Wrighton
nmdc:sty-11-33fbta56	Peatland microbial communities from Minnesota, USA, analyzing carbon cycling and trace gas fluxes	Christopher Schadt
nmdc:sty-11-aygzgv51	Riverbed sediment microbial communities from the Columbia River, Washington, USA	James Stegen
```

## Conclusion

In this how-to guide, we showed you how you could convert an NMDC Runtime API response from JSON into a couple different formats; specifically, CSV and TSV.
