# Processing of Mediavida forums

```
python get_mediavida_articles.py --input_link https://www.mediavida.com/foro/off-topic --output_folder corpus --num_pages 1500
python get_mediavida_comments.py --articles_metadata_folder corpus --output_folder corpus
python clean_comments.py --input_comment_folder corpus --output_folder output_final --output_folder_verbose output_final_verbose
```
