
rule bioformats_to_raw:
    input:
        lambda wildcard: gen_rule_input_path(wildcard)
    output:
        directory(temp("raw_slides/{slide}.raw"))
    log:
        "raw_slides/{slide}.log"
    benchmark:
        "raw_slides/{slide}.bench"
    params:
        log_level = config['log_level'],
        max_cached_tiles = config.get('tiff', {}).get('max_cached_tiles', 64),
        memo_directory = lambda _: get_tmp_dir(),
        tile_height = config['output']['tile_height'],
        tile_width = config['output']['tile_width'],
        workers = lambda _, threads: round(1.5 * threads),
    container:
        "docker://ilveroluca/bioformats2raw:0.3.1"
    resources:
        mem_mb = 5000,
        tmpdir = lambda _: get_tmp_dir()
    threads:
        4
    shell:
        """
        mkdir -p $(dirname {output:q}) &&
        bioformats2raw \
            --log-level={params.log_level} \
            --max_workers={params.workers} \
            --tile_height={params.tile_height} \
            --tile_width={params.tile_width} \
            --memo-directory={params.memo_directory} \
            --max_cached_tiles={params.max_cached_tiles} \
            {input:q} {output:q} &> {log:q}
        """


rule raw_to_ometiff:
    priority: 10  # higher than other rules
    input:
        "raw_slides/{slide}.raw"
    output:
        protected("tiffs/{slide}.ome.tiff")
    log:
        "tiffs/{slide}.log"
    benchmark:
        "tiffs/{slide}.bench"
    params:
        compression = config['output']['compression'],
        quality = config['output']['quality'],
        workers = lambda _, threads: threads,
        log_level = config['log_level']
    priority: 10
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    resources:
        mem_mb = 3000,
        tmpdir = lambda _: get_tmp_dir()
    threads:
        4
    shell:
        """
        mkdir -p $(dirname {output:q}) &&
        raw2ometiff \
            --compression={params.compression:q} \
            --quality={params.quality} \
            --log-level={params.log_level} \
            --max_workers={params.workers} \
            {input:q} {output:q} &> {log:q}
        """
