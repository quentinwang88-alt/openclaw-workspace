# OSS Cutover

`auto_mixcut` supports two OSS providers.

## Local Provider

Default mode. Files are stored under `AUTO_MIXCUT_OSS_ROOT`.

```bash
AUTO_MIXCUT_OSS_PROVIDER=local
AUTO_MIXCUT_OSS_ROOT=/Volumes/移动存储空间1/MigratedLocalStorage/Users/likeu3/auto_mixcut/var/oss
```

## Aliyun OSS Provider

Use this only after the local flow is stable and the bucket is ready.

```bash
AUTO_MIXCUT_OSS_PROVIDER=aliyun
AUTO_MIXCUT_OSS_BUCKET=your-bucket
AUTO_MIXCUT_ALIYUN_OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
AUTO_MIXCUT_ALIYUN_ACCESS_KEY_ID=your-access-key-id
AUTO_MIXCUT_ALIYUN_ACCESS_KEY_SECRET=your-access-key-secret

# Optional.
AUTO_MIXCUT_ALIYUN_SECURITY_TOKEN=your-sts-token
AUTO_MIXCUT_ALIYUN_OSS_PUBLIC_BASE_URL=https://your-cdn-or-custom-domain
AUTO_MIXCUT_OSS_CACHE_ROOT=/tmp/auto_mixcut/oss_cache
```

Legacy variable names are also accepted:

```bash
ALIYUN_OSS_BUCKET=your-bucket
ALIYUN_OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
ALIYUN_OSS_ACCESS_KEY_ID=your-access-key-id
ALIYUN_OSS_ACCESS_KEY_SECRET=your-access-key-secret
```

## Runtime Behavior

- Upload writes to the configured provider.
- Upload validates `file_size` and stores local SHA-256 in `oss_objects.file_hash`.
- Aliyun uploads also write SHA-256 as object metadata.
- FFmpeg and LLM steps resolve objects through a local cache:
  - local mode reads from `AUTO_MIXCUT_OSS_ROOT`;
  - aliyun mode downloads objects into `AUTO_MIXCUT_OSS_CACHE_ROOT` as needed.
- Signed preview URLs come from:
  - local preview server in local mode;
  - Aliyun signed URL in aliyun mode;
  - `AUTO_MIXCUT_ALIYUN_OSS_PUBLIC_BASE_URL` if configured.

## Cutover Checklist

1. Create the Aliyun OSS bucket.
2. Configure lifecycle rules for `raw`, `segments`, `frames`, `outputs`, `manifests`, and `temp`.
3. Add the environment variables above.
4. Run a single product through `upload -> probe -> watermark-check -> segment`.
5. Verify `oss_objects.bucket`, `object_key`, `file_size`, and `file_hash`.
6. Render one batch and verify Feishu preview URLs.
7. Only then switch scheduled/batch jobs to `AUTO_MIXCUT_OSS_PROVIDER=aliyun`.
