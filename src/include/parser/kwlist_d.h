/*-------------------------------------------------------------------------
 *
 * kwlist_d.h
 *    List of keywords represented as a ScanKeywordList.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/tools/gen_keywordlist.pl
 *
 *-------------------------------------------------------------------------
 */

#ifndef KWLIST_D_H
#define KWLIST_D_H

#include "parser/kwlookup.h"

static const char ScanKeywords_kw_string[] =
	"abort\0"
	"absolute\0"
	"access\0"
	"account\0"
	"action\0"
	"add\0"
	"admin\0"
	"after\0"
	"aggregate\0"
	"algorithm\0"
	"all\0"
	"also\0"
	"alter\0"
	"always\0"
	"analyse\0"
	"analyze\0"
	"and\0"
	"any\0"
	"app\0"
	"append\0"
	"archive\0"
	"array\0"
	"as\0"
	"asc\0"
	"assertion\0"
	"assignment\0"
	"asymmetric\0"
	"at\0"
	"attribute\0"
	"audit\0"
	"authid\0"
	"authorization\0"
	"auto_increment\0"
	"autoextend\0"
	"automapped\0"
	"backward\0"
	"barrier\0"
	"before\0"
	"begin\0"
	"begin_non_anoyblock\0"
	"between\0"
	"bigint\0"
	"binary\0"
	"binary_double\0"
	"binary_integer\0"
	"bit\0"
	"blanks\0"
	"blob\0"
	"blockchain\0"
	"body\0"
	"boolean\0"
	"both\0"
	"bucketcnt\0"
	"buckets\0"
	"by\0"
	"byteawithoutorder\0"
	"byteawithoutorderwithequal\0"
	"cache\0"
	"call\0"
	"called\0"
	"cancelable\0"
	"cascade\0"
	"cascaded\0"
	"case\0"
	"cast\0"
	"catalog\0"
	"catalog_name\0"
	"chain\0"
	"change\0"
	"char\0"
	"character\0"
	"characteristics\0"
	"characterset\0"
	"charset\0"
	"check\0"
	"checkpoint\0"
	"class\0"
	"class_origin\0"
	"clean\0"
	"client\0"
	"client_master_key\0"
	"client_master_keys\0"
	"clob\0"
	"close\0"
	"cluster\0"
	"coalesce\0"
	"collate\0"
	"collation\0"
	"column\0"
	"column_encryption_key\0"
	"column_encryption_keys\0"
	"column_name\0"
	"columns\0"
	"comment\0"
	"comments\0"
	"commit\0"
	"committed\0"
	"compact\0"
	"compatible_illegal_chars\0"
	"complete\0"
	"completion\0"
	"compress\0"
	"concurrently\0"
	"condition\0"
	"configuration\0"
	"connect\0"
	"connection\0"
	"consistent\0"
	"constant\0"
	"constraint\0"
	"constraint_catalog\0"
	"constraint_name\0"
	"constraint_schema\0"
	"constraints\0"
	"content\0"
	"continue\0"
	"contview\0"
	"conversion\0"
	"convert\0"
	"coordinator\0"
	"coordinators\0"
	"copy\0"
	"cost\0"
	"create\0"
	"cross\0"
	"csn\0"
	"csv\0"
	"cube\0"
	"current\0"
	"current_catalog\0"
	"current_date\0"
	"current_role\0"
	"current_schema\0"
	"current_time\0"
	"current_timestamp\0"
	"current_user\0"
	"cursor\0"
	"cursor_name\0"
	"cycle\0"
	"data\0"
	"database\0"
	"datafile\0"
	"datanode\0"
	"datanodes\0"
	"datatype_cl\0"
	"date\0"
	"date_format\0"
	"day\0"
	"day_hour\0"
	"day_minute\0"
	"day_second\0"
	"dbcompatibility\0"
	"deallocate\0"
	"dec\0"
	"decimal\0"
	"declare\0"
	"decode\0"
	"default\0"
	"defaults\0"
	"deferrable\0"
	"deferred\0"
	"definer\0"
	"delete\0"
	"delimiter\0"
	"delimiters\0"
	"delta\0"
	"deltamerge\0"
	"desc\0"
	"deterministic\0"
	"diagnostics\0"
	"dictionary\0"
	"direct\0"
	"directory\0"
	"disable\0"
	"discard\0"
	"disconnect\0"
	"distinct\0"
	"distribute\0"
	"distribution\0"
	"do\0"
	"document\0"
	"domain\0"
	"double\0"
	"drop\0"
	"dumpfile\0"
	"duplicate\0"
	"each\0"
	"elastic\0"
	"else\0"
	"enable\0"
	"enclosed\0"
	"encoding\0"
	"encrypted\0"
	"encrypted_value\0"
	"encryption\0"
	"encryption_type\0"
	"end\0"
	"ends\0"
	"enforced\0"
	"enum\0"
	"eol\0"
	"errors\0"
	"escape\0"
	"escaped\0"
	"escaping\0"
	"event\0"
	"events\0"
	"every\0"
	"except\0"
	"exchange\0"
	"exclude\0"
	"excluded\0"
	"excluding\0"
	"exclusive\0"
	"execute\0"
	"exists\0"
	"expired\0"
	"explain\0"
	"extension\0"
	"external\0"
	"extract\0"
	"false\0"
	"family\0"
	"fast\0"
	"features\0"
	"fenced\0"
	"fetch\0"
	"fields\0"
	"fileheader\0"
	"fill_missing_fields\0"
	"filler\0"
	"filter\0"
	"first\0"
	"fixed\0"
	"float\0"
	"following\0"
	"follows\0"
	"for\0"
	"force\0"
	"foreign\0"
	"formatter\0"
	"forward\0"
	"freeze\0"
	"from\0"
	"full\0"
	"function\0"
	"functions\0"
	"generated\0"
	"get\0"
	"global\0"
	"grant\0"
	"granted\0"
	"greatest\0"
	"group\0"
	"grouping\0"
	"groupparent\0"
	"handler\0"
	"having\0"
	"hdfsdirectory\0"
	"header\0"
	"hold\0"
	"hour\0"
	"hour_minute\0"
	"hour_second\0"
	"identified\0"
	"identity\0"
	"if\0"
	"ignore\0"
	"ignore_extra_data\0"
	"ilike\0"
	"immediate\0"
	"immutable\0"
	"implicit\0"
	"in\0"
	"include\0"
	"including\0"
	"increment\0"
	"incremental\0"
	"index\0"
	"indexes\0"
	"infile\0"
	"inherit\0"
	"inherits\0"
	"initial\0"
	"initially\0"
	"initrans\0"
	"inline\0"
	"inner\0"
	"inout\0"
	"input\0"
	"insensitive\0"
	"insert\0"
	"instead\0"
	"int\0"
	"integer\0"
	"internal\0"
	"intersect\0"
	"interval\0"
	"into\0"
	"invisible\0"
	"invoker\0"
	"ip\0"
	"is\0"
	"isnull\0"
	"isolation\0"
	"join\0"
	"key\0"
	"key_path\0"
	"key_store\0"
	"kill\0"
	"label\0"
	"language\0"
	"large\0"
	"last\0"
	"lc_collate\0"
	"lc_ctype\0"
	"leading\0"
	"leakproof\0"
	"least\0"
	"left\0"
	"less\0"
	"level\0"
	"like\0"
	"limit\0"
	"lines\0"
	"list\0"
	"listen\0"
	"load\0"
	"local\0"
	"localtime\0"
	"localtimestamp\0"
	"location\0"
	"lock\0"
	"locked\0"
	"log\0"
	"logging\0"
	"login_any\0"
	"login_failure\0"
	"login_success\0"
	"logout\0"
	"loop\0"
	"mapping\0"
	"masking\0"
	"master\0"
	"match\0"
	"matched\0"
	"materialized\0"
	"maxextents\0"
	"maxsize\0"
	"maxtrans\0"
	"maxvalue\0"
	"merge\0"
	"message_text\0"
	"minextents\0"
	"minus\0"
	"minute\0"
	"minute_second\0"
	"minvalue\0"
	"mode\0"
	"model\0"
	"modify\0"
	"month\0"
	"move\0"
	"movement\0"
	"mysql_errno\0"
	"name\0"
	"names\0"
	"national\0"
	"natural\0"
	"nchar\0"
	"next\0"
	"no\0"
	"nocompress\0"
	"nocycle\0"
	"node\0"
	"nologging\0"
	"nomaxvalue\0"
	"nominvalue\0"
	"none\0"
	"not\0"
	"nothing\0"
	"notify\0"
	"notnull\0"
	"nowait\0"
	"null\0"
	"nullcols\0"
	"nullif\0"
	"nulls\0"
	"number\0"
	"numeric\0"
	"numstr\0"
	"nvarchar\0"
	"nvarchar2\0"
	"nvl\0"
	"object\0"
	"of\0"
	"off\0"
	"offset\0"
	"oids\0"
	"on\0"
	"only\0"
	"operator\0"
	"optimization\0"
	"option\0"
	"optionally\0"
	"options\0"
	"or\0"
	"order\0"
	"out\0"
	"outer\0"
	"outfile\0"
	"over\0"
	"overlaps\0"
	"overlay\0"
	"owned\0"
	"owner\0"
	"package\0"
	"packages\0"
	"parser\0"
	"partial\0"
	"partition\0"
	"partitions\0"
	"passing\0"
	"password\0"
	"pctfree\0"
	"per\0"
	"percent\0"
	"performance\0"
	"perm\0"
	"placing\0"
	"plan\0"
	"plans\0"
	"policy\0"
	"pool\0"
	"position\0"
	"precedes\0"
	"preceding\0"
	"precision\0"
	"predict\0"
	"preferred\0"
	"prefix\0"
	"prepare\0"
	"prepared\0"
	"preserve\0"
	"primary\0"
	"prior\0"
	"priorer\0"
	"private\0"
	"privilege\0"
	"privileges\0"
	"procedural\0"
	"procedure\0"
	"profile\0"
	"publication\0"
	"publish\0"
	"purge\0"
	"query\0"
	"quote\0"
	"randomized\0"
	"range\0"
	"ratio\0"
	"raw\0"
	"read\0"
	"real\0"
	"reassign\0"
	"rebuild\0"
	"recheck\0"
	"recursive\0"
	"recyclebin\0"
	"redisanyvalue\0"
	"ref\0"
	"references\0"
	"refresh\0"
	"reindex\0"
	"reject\0"
	"relative\0"
	"release\0"
	"reloptions\0"
	"remote\0"
	"remove\0"
	"rename\0"
	"repeat\0"
	"repeatable\0"
	"replace\0"
	"replica\0"
	"reset\0"
	"resize\0"
	"resource\0"
	"restart\0"
	"restrict\0"
	"return\0"
	"returned_sqlstate\0"
	"returning\0"
	"returns\0"
	"reuse\0"
	"revoke\0"
	"right\0"
	"role\0"
	"roles\0"
	"rollback\0"
	"rollup\0"
	"rotation\0"
	"row\0"
	"row_count\0"
	"rownum\0"
	"rows\0"
	"rowtype\0"
	"rule\0"
	"sample\0"
	"savepoint\0"
	"schedule\0"
	"schema\0"
	"schema_name\0"
	"scroll\0"
	"search\0"
	"second\0"
	"security\0"
	"select\0"
	"separator\0"
	"sequence\0"
	"sequences\0"
	"serializable\0"
	"server\0"
	"session\0"
	"session_user\0"
	"set\0"
	"setof\0"
	"sets\0"
	"share\0"
	"shippable\0"
	"show\0"
	"shrink\0"
	"shutdown\0"
	"siblings\0"
	"similar\0"
	"simple\0"
	"size\0"
	"skip\0"
	"slave\0"
	"slice\0"
	"smalldatetime\0"
	"smalldatetime_format\0"
	"smallint\0"
	"snapshot\0"
	"some\0"
	"source\0"
	"space\0"
	"spill\0"
	"split\0"
	"sql\0"
	"stable\0"
	"stacked\0"
	"standalone\0"
	"start\0"
	"starting\0"
	"starts\0"
	"statement\0"
	"statement_id\0"
	"statistics\0"
	"stdin\0"
	"stdout\0"
	"storage\0"
	"store\0"
	"stored\0"
	"stratify\0"
	"stream\0"
	"strict\0"
	"strip\0"
	"subclass_origin\0"
	"subpartition\0"
	"subpartitions\0"
	"subscription\0"
	"substring\0"
	"symmetric\0"
	"synonym\0"
	"sys_refcursor\0"
	"sysdate\0"
	"sysid\0"
	"system\0"
	"table\0"
	"table_name\0"
	"tables\0"
	"tablesample\0"
	"tablespace\0"
	"target\0"
	"temp\0"
	"template\0"
	"temporary\0"
	"terminated\0"
	"text\0"
	"than\0"
	"then\0"
	"time\0"
	"time_format\0"
	"timecapsule\0"
	"timestamp\0"
	"timestamp_format\0"
	"timestampdiff\0"
	"tinyint\0"
	"to\0"
	"trailing\0"
	"transaction\0"
	"transform\0"
	"treat\0"
	"trigger\0"
	"trim\0"
	"true\0"
	"truncate\0"
	"trusted\0"
	"tsfield\0"
	"tstag\0"
	"tstime\0"
	"type\0"
	"types\0"
	"unbounded\0"
	"uncommitted\0"
	"unencrypted\0"
	"union\0"
	"unique\0"
	"unknown\0"
	"unlimited\0"
	"unlisten\0"
	"unlock\0"
	"unlogged\0"
	"until\0"
	"unusable\0"
	"update\0"
	"use\0"
	"useeof\0"
	"user\0"
	"using\0"
	"vacuum\0"
	"valid\0"
	"validate\0"
	"validation\0"
	"validator\0"
	"value\0"
	"values\0"
	"varchar\0"
	"varchar2\0"
	"variables\0"
	"variadic\0"
	"varying\0"
	"vcgroup\0"
	"verbose\0"
	"verify\0"
	"version\0"
	"view\0"
	"visible\0"
	"volatile\0"
	"wait\0"
	"warnings\0"
	"weak\0"
	"when\0"
	"where\0"
	"while\0"
	"whitespace\0"
	"window\0"
	"with\0"
	"within\0"
	"without\0"
	"work\0"
	"workload\0"
	"wrapper\0"
	"write\0"
	"xml\0"
	"xmlattributes\0"
	"xmlconcat\0"
	"xmlelement\0"
	"xmlexists\0"
	"xmlforest\0"
	"xmlparse\0"
	"xmlpi\0"
	"xmlroot\0"
	"xmlserialize\0"
	"year\0"
	"year_month\0"
	"yes\0"
	"zone";

static const uint16 ScanKeywords_kw_offsets[] = {
	0,
	6,
	15,
	22,
	30,
	37,
	41,
	47,
	53,
	63,
	73,
	77,
	82,
	88,
	95,
	103,
	111,
	115,
	119,
	123,
	130,
	138,
	144,
	147,
	151,
	161,
	172,
	183,
	186,
	196,
	202,
	209,
	223,
	238,
	249,
	260,
	269,
	277,
	284,
	290,
	310,
	318,
	325,
	332,
	346,
	361,
	365,
	372,
	377,
	388,
	393,
	401,
	406,
	416,
	424,
	427,
	445,
	472,
	478,
	483,
	490,
	501,
	509,
	518,
	523,
	528,
	536,
	549,
	555,
	562,
	567,
	577,
	593,
	606,
	614,
	620,
	631,
	637,
	650,
	656,
	663,
	681,
	700,
	705,
	711,
	719,
	728,
	736,
	746,
	753,
	775,
	798,
	810,
	818,
	826,
	835,
	842,
	852,
	860,
	885,
	894,
	905,
	914,
	927,
	937,
	951,
	959,
	970,
	981,
	990,
	1001,
	1020,
	1036,
	1054,
	1066,
	1074,
	1083,
	1092,
	1103,
	1111,
	1123,
	1136,
	1141,
	1146,
	1153,
	1159,
	1163,
	1167,
	1172,
	1180,
	1196,
	1209,
	1222,
	1237,
	1250,
	1268,
	1281,
	1288,
	1300,
	1306,
	1311,
	1320,
	1329,
	1338,
	1348,
	1360,
	1365,
	1377,
	1381,
	1390,
	1401,
	1412,
	1428,
	1439,
	1443,
	1451,
	1459,
	1466,
	1474,
	1483,
	1494,
	1503,
	1511,
	1518,
	1528,
	1539,
	1545,
	1556,
	1561,
	1575,
	1587,
	1598,
	1605,
	1615,
	1623,
	1631,
	1642,
	1651,
	1662,
	1675,
	1678,
	1687,
	1694,
	1701,
	1706,
	1715,
	1725,
	1730,
	1738,
	1743,
	1750,
	1759,
	1768,
	1778,
	1794,
	1805,
	1821,
	1825,
	1830,
	1839,
	1844,
	1848,
	1855,
	1862,
	1870,
	1879,
	1885,
	1892,
	1898,
	1905,
	1914,
	1922,
	1931,
	1941,
	1951,
	1959,
	1966,
	1974,
	1982,
	1992,
	2001,
	2009,
	2015,
	2022,
	2027,
	2036,
	2043,
	2049,
	2056,
	2067,
	2087,
	2094,
	2101,
	2107,
	2113,
	2119,
	2129,
	2137,
	2141,
	2147,
	2155,
	2165,
	2173,
	2180,
	2185,
	2190,
	2199,
	2209,
	2219,
	2223,
	2230,
	2236,
	2244,
	2253,
	2259,
	2268,
	2280,
	2288,
	2295,
	2309,
	2316,
	2321,
	2326,
	2338,
	2350,
	2361,
	2370,
	2373,
	2380,
	2398,
	2404,
	2414,
	2424,
	2433,
	2436,
	2444,
	2454,
	2464,
	2476,
	2482,
	2490,
	2497,
	2505,
	2514,
	2522,
	2532,
	2541,
	2548,
	2554,
	2560,
	2566,
	2578,
	2585,
	2593,
	2597,
	2605,
	2614,
	2624,
	2633,
	2638,
	2648,
	2656,
	2659,
	2662,
	2669,
	2679,
	2684,
	2688,
	2697,
	2707,
	2712,
	2718,
	2727,
	2733,
	2738,
	2749,
	2758,
	2766,
	2776,
	2782,
	2787,
	2792,
	2798,
	2803,
	2809,
	2815,
	2820,
	2827,
	2832,
	2838,
	2848,
	2863,
	2872,
	2877,
	2884,
	2888,
	2896,
	2906,
	2920,
	2934,
	2941,
	2946,
	2954,
	2962,
	2969,
	2975,
	2983,
	2996,
	3007,
	3015,
	3024,
	3033,
	3039,
	3052,
	3063,
	3069,
	3076,
	3090,
	3099,
	3104,
	3110,
	3117,
	3123,
	3128,
	3137,
	3149,
	3154,
	3160,
	3169,
	3177,
	3183,
	3188,
	3191,
	3202,
	3210,
	3215,
	3225,
	3236,
	3247,
	3252,
	3256,
	3264,
	3271,
	3279,
	3286,
	3291,
	3300,
	3307,
	3313,
	3320,
	3328,
	3335,
	3344,
	3354,
	3358,
	3365,
	3368,
	3372,
	3379,
	3384,
	3387,
	3392,
	3401,
	3414,
	3421,
	3432,
	3440,
	3443,
	3449,
	3453,
	3459,
	3467,
	3472,
	3481,
	3489,
	3495,
	3501,
	3509,
	3518,
	3525,
	3533,
	3543,
	3554,
	3562,
	3571,
	3579,
	3583,
	3591,
	3603,
	3608,
	3616,
	3621,
	3627,
	3634,
	3639,
	3648,
	3657,
	3667,
	3677,
	3685,
	3695,
	3702,
	3710,
	3719,
	3728,
	3736,
	3742,
	3750,
	3758,
	3768,
	3779,
	3790,
	3800,
	3808,
	3820,
	3828,
	3834,
	3840,
	3846,
	3857,
	3863,
	3869,
	3873,
	3878,
	3883,
	3892,
	3900,
	3908,
	3918,
	3929,
	3943,
	3947,
	3958,
	3966,
	3974,
	3981,
	3990,
	3998,
	4009,
	4016,
	4023,
	4030,
	4037,
	4048,
	4056,
	4064,
	4070,
	4077,
	4086,
	4094,
	4103,
	4110,
	4128,
	4138,
	4146,
	4152,
	4159,
	4165,
	4170,
	4176,
	4185,
	4192,
	4201,
	4205,
	4215,
	4222,
	4227,
	4235,
	4240,
	4247,
	4257,
	4266,
	4273,
	4285,
	4292,
	4299,
	4306,
	4315,
	4322,
	4332,
	4341,
	4351,
	4364,
	4371,
	4379,
	4392,
	4396,
	4402,
	4407,
	4413,
	4423,
	4428,
	4435,
	4444,
	4453,
	4461,
	4468,
	4473,
	4478,
	4484,
	4490,
	4504,
	4525,
	4534,
	4543,
	4548,
	4555,
	4561,
	4567,
	4573,
	4577,
	4584,
	4592,
	4603,
	4609,
	4618,
	4625,
	4635,
	4648,
	4659,
	4665,
	4672,
	4680,
	4686,
	4693,
	4702,
	4709,
	4716,
	4722,
	4738,
	4751,
	4765,
	4778,
	4788,
	4798,
	4806,
	4820,
	4828,
	4834,
	4841,
	4847,
	4858,
	4865,
	4877,
	4888,
	4895,
	4900,
	4909,
	4919,
	4930,
	4935,
	4940,
	4945,
	4950,
	4962,
	4974,
	4984,
	5001,
	5015,
	5023,
	5026,
	5035,
	5047,
	5057,
	5063,
	5071,
	5076,
	5081,
	5090,
	5098,
	5106,
	5112,
	5119,
	5124,
	5130,
	5140,
	5152,
	5164,
	5170,
	5177,
	5185,
	5195,
	5204,
	5211,
	5220,
	5226,
	5235,
	5242,
	5246,
	5253,
	5258,
	5264,
	5271,
	5277,
	5286,
	5297,
	5307,
	5313,
	5320,
	5328,
	5337,
	5347,
	5356,
	5364,
	5372,
	5380,
	5387,
	5395,
	5400,
	5408,
	5417,
	5422,
	5431,
	5436,
	5441,
	5447,
	5453,
	5464,
	5471,
	5476,
	5483,
	5491,
	5496,
	5505,
	5513,
	5519,
	5523,
	5537,
	5547,
	5558,
	5568,
	5578,
	5587,
	5593,
	5601,
	5614,
	5619,
	5630,
	5634,
};

#define SCANKEYWORDS_NUM_KEYWORDS 679

static int
ScanKeywords_hash_func(const void *key, size_t keylen)
{
	static const int16 h[1359] = {
		32767, 32767, 64,    -347,  0,     32767, 26,    32767,
		260,   32767, 466,   619,   0,     0,     32767, 420,
		195,   6,     32767, 32767, 32767, 617,   32767, 488,
		236,   647,   32767, 0,     32767, 178,   0,     32767,
		32767, 343,   234,   272,   32767, 32767, 383,   138,
		558,   32767, 32767, 632,   -80,   326,   345,   32767,
		32767, 32767, 109,   32767, 321,   441,   0,     73,
		146,   32767, 563,   148,   383,   0,     32767, 32767,
		32767, 32767, 130,   1177,  32767, 554,   0,     -200,
		0,     91,    407,   0,     446,   280,   0,     0,
		203,   171,   454,   16,    246,   650,   32767, 270,
		-134,  898,   395,   -531,  32767, 590,   0,     32767,
		0,     32767, 399,   32767, -417,  -24,   206,   32767,
		-555,  0,     133,   36,    374,   658,   -31,   0,
		222,   0,     -24,   32767, 32767, 32767, 402,   184,
		32767, 32767, 433,   -363,  101,   32767, 0,     351,
		0,     -220,  730,   510,   32767, 282,   668,   32767,
		-283,  32767, 32767, 75,    32767, 244,   0,     32767,
		32767, 0,     -228,  32767, 157,   -202,  1,     -290,
		0,     0,     326,   -484,  198,   333,   376,   434,
		-309,  -72,   646,   -55,   270,   32767, -261,  311,
		-513,  384,   794,   0,     -950,  32767, 684,   125,
		439,   9,     0,     -451,  -163,  32767, 32767, -257,
		535,   32767, 32767, -95,   0,     280,   508,   -630,
		493,   -536,  32767, 394,   32767, -73,   32767, 32767,
		377,   32767, 344,   577,   503,   32767, 32767, 21,
		32767, 285,   32767, 232,   -64,   572,   7,     497,
		0,     0,     32767, 450,   32767, 566,   353,   32767,
		0,     -118,  421,   129,   195,   32767, 276,   336,
		618,   0,     -75,   0,     -15,   414,   -7,    32767,
		0,     32767, 32767, -225,  32767, 32767, 0,     444,
		77,    32767, 32767, 190,   0,     31,    170,   -137,
		32767, 32767, 16,    0,     -4,    302,   199,   32767,
		485,   32767, 801,   -42,   32767, 184,   32767, 32767,
		-593,  178,   32767, 25,    0,     0,     634,   17,
		52,    32767, 223,   136,   32767, 413,   32767, 191,
		32767, 115,   -192,  544,   32767, 32767, 0,     -7,
		32767, -480,  995,   166,   603,   449,   32767, 32767,
		0,     -712,  518,   32767, 34,    427,   32767, 39,
		352,   0,     32767, 287,   0,     -310,  512,   1169,
		32767, -358,  -234,  -210,  -270,  45,    417,   -306,
		510,   32767, -802,  499,   324,   349,   -953,  32767,
		724,   -754,  0,     -62,   32767, 564,   248,   248,
		343,   600,   32767, 615,   32767, 621,   1067,  216,
		32767, 736,   155,   0,     32767, 32767, -241,  32767,
		32767, 431,   0,     1396,  32767, 573,   434,   32767,
		32767, -124,  278,   -600,  32767, 431,   -316,  32767,
		353,   477,   0,     357,   998,   0,     -167,  32767,
		-264,  32767, 284,   464,   32767, 32767, 872,   445,
		160,   32767, -844,  153,   684,   -295,  32767, 32767,
		-143,  435,   612,   32767, 32767, -180,  425,   32767,
		32767, 32767, 32767, 228,   32767, 32767, 32767, 32767,
		667,   32767, -268,  100,   32767, -181,  515,   32767,
		32767, 32767, 247,   -12,   -70,   -887,  0,     155,
		32767, -436,  32767, 32767, 525,   -576,  238,   -20,
		-265,  53,    32767, 32767, 395,   512,   0,     327,
		32767, 32767, 32767, 500,   0,     32767, 32767, 32767,
		631,   664,   32767, 580,   0,     0,     103,   32767,
		32767, 0,     276,   -331,  32767, -508,  575,   331,
		638,   165,   32767, 32767, 283,   -158,  32767, 32767,
		0,     57,    311,   32767, 0,     32767, -148,  33,
		332,   277,   574,   32767, 156,   -17,   32767, 624,
		32767, -703,  316,   443,   32767, 95,    41,    347,
		94,    -34,   0,     32767, 0,     -554,  381,   32767,
		32767, 32767, 0,     32767, 0,     32767, 372,   32767,
		170,   234,   32767, 32767, 32767, 32767, 32767, 1047,
		0,     -812,  32767, -534,  0,     0,     32767, 627,
		0,     -116,  321,   310,   -101,  49,    -62,   32767,
		323,   32767, 359,   71,    364,   32767, 32767, 32767,
		558,   673,   219,   385,   182,   -384,  405,   0,
		32767, -463,  32767, 32767, 32767, 134,   32767, 3,
		-261,  32767, 48,    752,   253,   32767, 0,     0,
		32767, 32767, 242,   0,     -527,  557,   202,   -398,
		-59,   -16,   0,     32767, 32767, 669,   68,    32767,
		959,   127,   32767, 32767, 320,   0,     304,   -21,
		32767, 250,   0,     -78,   144,   32767, 32767, 637,
		0,     84,    -112,  32767, 32767, 32767, 0,     32767,
		15,    -516,  32767, -479,  -213,  32767, 538,   32767,
		-148,  32767, 32767, 32767, 81,    -54,   0,     -711,
		390,   0,     70,    32767, 660,   32767, 0,     -310,
		0,     0,     200,   32767, 0,     218,   32767, 0,
		0,     0,     57,    32767, 597,   32767, 158,   0,
		245,   465,   648,   32767, 101,   32767, 939,   32767,
		32767, 32767, 61,    -218,  551,   32767, 676,   0,
		32767, 519,   0,     358,   457,   360,   32767, 0,
		32767, 226,   0,     -113,  32767, 0,     0,     0,
		4,     116,   419,   32767, 188,   202,   285,   271,
		32767, 118,   32767, 634,   306,   425,   0,     32767,
		577,   176,   74,    32767, -207,  0,     0,     32767,
		-259,  32767, 0,     -17,   32767, -98,   32767, 1004,
		32767, 468,   32767, 32767, -48,   472,   32767, 32767,
		0,     -143,  32767, -348,  32767, 0,     32767, 32767,
		-147,  0,     32767, 0,     120,   459,   624,   240,
		32767, 0,     32767, 32767, 147,   523,   0,     516,
		32767, 32767, 32767, 138,   -174,  32767, 496,   32767,
		0,     0,     32767, 0,     32767, -244,  397,   32767,
		0,     663,   0,     0,     0,     93,    32767, 0,
		32767, 32767, 633,   32767, 32767, 32767, 80,    0,
		308,   32767, 284,   32767, 0,     0,     238,   -23,
		142,   -34,   355,   32767, 124,   977,   -76,   39,
		71,    32767, 589,   32767, 255,   236,   90,    0,
		196,   32767, 161,   0,     0,     32767, 32767, 32767,
		0,     32767, 0,     644,   491,   32767, 490,   -419,
		32767, 628,   613,   32767, 32767, 339,   0,     32767,
		571,   -162,  480,   32767, 74,    0,     -157,  0,
		350,   50,    32767, 174,   213,   124,   -239,  424,
		657,   469,   32767, 559,   32767, 0,     32767, 32767,
		32767, 95,    184,   32767, 667,   32767, -473,  32767,
		0,     0,     -63,   32767, 164,   -189,  430,   -241,
		32767, 0,     164,   0,     120,   32767, -242,  711,
		32767, 0,     99,    32767, 32767, 32767, 357,   0,
		32767, 32767, 32767, 0,     32767, 32767, 47,    24,
		0,     292,   32767, 32767, 84,    326,   32767, 451,
		-63,   139,   32767, 32767, 32767, 3,     32767, 32767,
		346,   -521,  -362,  295,   32767, 21,    0,     32767,
		391,   32767, 32767, 330,   402,   32767, 32767, 32767,
		32767, 455,   32767, 32767, 717,   722,   32767, 0,
		32767, 88,    32767, 32767, 124,   32767, 32767, 32767,
		32767, 32767, 0,     0,     446,   32767, 32767, 127,
		32767, 168,   68,    32767, 32767, -592,  81,    -734,
		0,     32767, 221,   32767, 232,   32767, 547,   0,
		550,   849,   0,     387,   32767, 32767, 32767, 207,
		-321,  664,   0,     644,   32767, 0,     378,   -7,
		0,     32767, 0,     32767, 228,   -48,   111,   581,
		-86,   32767, 32767, -33,   -99,   32767, 0,     0,
		32767, 0,     0,     32767, -411,  225,   441,   35,
		0,     32767, 32767, 32767, 32767, 82,    414,   566,
		0,     440,   -5,    -93,   0,     32767, 0,     32767,
		32767, 32767, 503,   0,     -151,  670,   32767, 32767,
		489,   32767, 32767, 217,   0,     473,   -225,  32767,
		481,   32767, 32767, 891,   32767, 1543,  605,   267,
		32767, -49,   32767, 566,   78,    0,     32767, 359,
		376,   180,   32767, 0,     0,     403,   32767, 32767,
		304,   130,   32767, 392,   0,     0,     146,   150,
		32767, 200,   -127,  32767, 0,     376,   32767, 32767,
		-160,  800,   589,   167,   -146,  0,     -25,   32767,
		0,     32767, 32767, 32767, 32767, 0,     0,     32767,
		0,     0,     32767, 0,     0,     32767, 0,     32767,
		32767, 32767, 32767, 0,     32767, 568,   112,   10,
		32767, 938,   294,   607,   32767, 10,    0,     32767,
		32767, 32767, 32767, 32767, 187,   444,   32767, 0,
		32767, 220,   32767, 321,   32767, 665,   0,     857,
		32767, 32767, 467,   42,    304,   451,   0,     0,
		0,     32767, 32767, 32767, 32767, 40,    0,     104,
		32767, 174,   0,     0,     0,     478,   0,     32767,
		32767, 820,   106,   32767, 32767, 0,     0,     72,
		0,     -82,   32767, 205,   -234,  32767, 0,     0,
		-169,  327,   300,   0,     -295,  -363,  32767, 32767,
		32767, 32767, 603,   32767, 32767, 96,    32767, 32767,
		65,    32767, 0,     32767, 56,    32767, 448,   670,
		32767, -167,  32767, 125,   212,   32767, 442,   0,
		32767, 32767, 0,     -83,   128,   0,     521,   32767,
		0,     32767, 0,     -331,  607,   32767, -496,  221,
		139,   486,   264,   338,   0,     32767, 398,   32767,
		0,     32767, 288,   418,   0,     808,   0,     429,
		32767, 446,   32767, 32767, 32767, 32767, 32767, 349,
		298,   0,     32767, 85,    678,   32767, 32767, 32767,
		32767, 32767, 146,   32767, 303,   0,     32767, 32767,
		404,   32767, 266,   32767, 32767, 0,     280,   0,
		32767, 32767, 273,   1191,  -20,   674,   366,   253,
		123,   682,   204,   0,     32767, 50,    0,     0,
		652,   194,   0,     935,   540,   475,   -420,  32767,
		32767, 0,     0,     616,   32767, -298,  262,   -575,
		32767, 446,   594,   0,     32767, 290,   32767, 532,
		461,   32767, 32767, 98,    32767, 517,   349,   0,
		0,     0,     32767, 0,     0,     0,     32767, 725,
		3,     595,   0,     32767, 32767, 372,   609,   0,
		49,    -66,   0,     565,   32767, 32767, 32767, 32767,
		242,   32767, 32767, 32767, 438,   179,   32767, 334,
		0,     -382,  312,   0,     0,     32767, 374
	};

	const unsigned char *k = (const unsigned char *) key;
	uint32		a = 0;
	uint32		b = 0;

	while (keylen--)
	{
		unsigned char c = *k++ | 0x20;

		a = a * 257 + c;
		b = b * 17 + c;
	}
	return h[a % 1359] + h[b % 1359];
}

const ScanKeywordList ScanKeywords = {
	ScanKeywords_kw_string,
	ScanKeywords_kw_offsets,
	ScanKeywords_hash_func,
	SCANKEYWORDS_NUM_KEYWORDS,
	26
};

#endif							/* KWLIST_D_H */
