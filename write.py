def write_to_file(df, trg_dir, format):
    '''

    :param df:
    :param trg_dir:
    :param format:
    :return: writes the dataframe 'df' to the target directory 'trg-dir'
            in the format given
    '''

    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'day'). \
        mode('append'). \
        format(format). \
        save(trg_dir)
