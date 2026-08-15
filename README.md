# packer

packer packs chunks into packs storing the metadata in MetadataStore and the data of chunks using DataStore. 

MetadataStore and DataStore are interfaces so that the metadata and data can be stored in different ways.
For example you could have the metadata in postgres and the 
data in s3 or metadata in a bolt database and the packs be local files

This library currently implements a MetadataStore using bolt and DataStore using local files.
They are located in the metadata/bolt and data/local directories. 
