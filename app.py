import os
from util import create_spark_session
from read import from_files
from process import transform
from write import write_to_file


def main():
    env = os.environ.get('ENVIRON')

    spark = create_spark_session(env, 'BeginnerSparkApp')

    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')

    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed = transform(df)
    write_to_file(df_transformed, tgt_dir, tgt_file_format)



if __name__ == '__main__':
    main()
