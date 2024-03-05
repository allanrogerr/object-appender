# Object Appender

`object-appender` gets and appends objects in memory, under an s3 bucket/prefix, then uploads the single resulting object to another s3 bucket/prefix

Usage:
```
go build
./object-appender --source-bucket-prefix <source-bucket-prefix> \
--target-bucket-prefix <target-bucket-prefix> \
--endpoint <endpoint> \
--accesskey <accesskey> \
--secretkey <secretkey> 
```
e.g.
```
go build
./object-appender --source-bucket-prefix "source-append-demo/2024/02/26" \
--target-bucket-prefix "target-append-demo/2024/02" \
--endpoint play.min.io:9000 \
--accesskey appendreadwrite \
--secretkey minio123

./object-appender --source-bucket-prefix "source-append-demo-object/" \
--target-bucket-prefix "target-append-demo-object/archive" \
--endpoint play.min.io:9000 \
--accesskey appendreadwrite \
--secretkey minio123
```

The first example comes with certain example parameters with arguments, assuming:
- at s3 endpoint `play.min.io:9000` (`endpoint`), there exists a source bucket/prefix `source-append-demo/2024/02/26` (`source-bucket-prefix`) which contains miscellaneous objects accessible using credentials `appendreadwrite` (`accesskey`) / `minio123` (`secretkey`)
- at the same endpoint, there exists a target bucket/prefix `target-append-demo/2024/02` (`target-bucket-prefix`)

The second example comes with certain example parameters with arguments, assuming:
- at s3 endpoint `play.min.io:9000` (`endpoint`), there exists a source bucket/prefix `source-append-demo-object/` (`source-bucket-prefix`) which contains miscellaneous objects accessible using credentials `appendreadwrite` (`accesskey`) / `minio123` (`secretkey`)
- at the same endpoint, there exists a target bucket/prefix `target-append-demo-object/archive` (`target-bucket-prefix`)

The program downloads then appends all objects from `source-bucket-prefix`, in no specific order, creating a single resulting object on the client. This resulting object is subsequently uploaded to `target-bucket-prefix`.