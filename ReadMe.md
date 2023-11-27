## S3 Migration Tool

 This codebase is a simple tool to migrate your s3 buckets from one provider to another one. 
 
 When making this for my specific use case, I wanted to keep in mind that this would be OSS and should be usable for all S3 providers. There was compatibility issues between backblaze and wasabi that I did not want to sort out, so it downloads everything to a `./tmp` dir then reuploads it instead of moving it directly. Yes, there are probably better ways to do this but this was quick and dirty. Hush, it deletes everything for you afterwards.

Enjoy :) 