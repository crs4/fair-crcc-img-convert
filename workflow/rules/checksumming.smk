
rule compute_tiff_checksum:
    input:
        tiff = "tiffs/{slide}.tiff"
    output:
        chksum = "tiffs/{slide}.tiff.sha"
    log:
        "tiffs/{slide}.tiff.sha.log"
    benchmark:
        "tiffs/{slide}.tiff.sha.bench"
    params:
        checksum_alg = 256
    resources:
        mem_mb = 64
    container:
        "docker://ilveroluca/raw2ometiff:0.3.0"
    shell:
        """
        sha{params.checksum_alg}sum {input:q} > {output:q} 2> {log:q}
        """

