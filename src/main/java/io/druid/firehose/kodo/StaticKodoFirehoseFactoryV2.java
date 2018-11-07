/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.kodo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import io.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
public class StaticKodoFirehoseFactoryV2 extends PrefetchableTextFilesFirehoseFactory<QiniuObject>
{
  private static final Logger log = new Logger(StaticKodoFirehoseFactoryV2.class);
  private static final long MAX_LISTING_LENGTH = 1000;

  private final List<URI> uris;
  private final List<URI> prefixes;
  private final String ak;
  private final String sk;
  private final String bucketUrl;


  @JsonIgnore
  private final Auth auth;
  @JsonIgnore
  private BucketManager bucketManager;

  @JsonCreator
  public StaticKodoFirehoseFactoryV2(
      @JsonProperty("ak") String ak,
      @JsonProperty("sk") String sk,
      @JsonProperty("bucketUrl") String bucketUrl,
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("prefixes") List<URI> prefixes,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    if (Strings.isNullOrEmpty(ak) || Strings.isNullOrEmpty(sk)) {
      throw new IAE("ak and sk cannot be empty");
    }
    this.ak = ak;
    this.sk = sk;
    this.auth = Auth.create(ak, sk);
    this.bucketManager = new BucketManager(auth);
    this.bucketUrl = bucketUrl.endsWith("/") ? bucketUrl : bucketUrl + "/";
    this.uris = uris == null ? new ArrayList<>() : uris;
    this.prefixes = prefixes == null ? new ArrayList<>() : prefixes;

    if (!this.uris.isEmpty() && !this.prefixes.isEmpty()) {
      throw new IAE("uris and prefixes cannot be used together");
    }

    if (this.uris.isEmpty() && this.prefixes.isEmpty()) {
      throw new IAE("uris o"
                    + ""
                    + ""
                    + "r prefixes must be specified");
    }

    for (final URI inputURI : this.uris) {
      Preconditions.checkArgument(inputURI.getScheme().equals("qiniu"), "input uri scheme == qiniu (%s)", inputURI);
    }

    for (final URI inputURI : this.prefixes) {
      Preconditions.checkArgument(inputURI.getScheme().equals("qiniu"), "input uri scheme == qiniu (%s)", inputURI);
    }
  }

  private static String extractS3Key(URI uri)
  {
    return uri.getPath().startsWith("/")
           ? uri.getPath().substring(1)
           : uri.getPath();
  }

  @JsonProperty
  public String getAk()
  {
    return ak;
  }

  @JsonProperty
  public String getSk()
  {
    return sk;
  }

  @JsonProperty("bucketUrl")
  public String getBucketUrl()
  {
    return bucketUrl;
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty("prefixes")
  public List<URI> getPrefixes()
  {
    return prefixes;
  }

  @Override
  protected Collection<QiniuObject> initObjects()
  {
    List<QiniuObject> objects = new ArrayList<>();

    for (URI uri : uris) {
      String bucket = uri.getAuthority();
      String s3Key = extractS3Key(uri);
      try {
        FileInfo stat = bucketManager.stat(bucket, s3Key);
        QiniuObject fileInfo = new QiniuObject(s3Key, bucket, bucketUrl + s3Key);
        objects.add(fileInfo);
      }
      catch (QiniuException e) {
        continue;
      }

    }

    for (URI uri : prefixes) {
      String bucket = uri.getAuthority();
      String s3Prefix = extractS3Key(uri);

      BucketManager.FileListIterator fileListIterator = bucketManager.createFileListIterator(
          bucket,
          s3Prefix,
          (int) MAX_LISTING_LENGTH,
          ""
      );
      while (fileListIterator.hasNext()) {
        FileInfo[] items = fileListIterator.next();
        for (FileInfo item : items) {
          objects.add(new QiniuObject(item.key, bucket, bucketUrl + item.key));
        }
      }
    }
    return objects;
  }

  @Override
  protected InputStream openObjectStream(QiniuObject object) throws IOException
  {
    // Get data of the given object and open an input stream
    return new URL(object.getUrl()).openStream();
  }

  @Override
  protected InputStream wrapObjectStream(QiniuObject object, InputStream stream) throws IOException
  {
    return object.getKey().endsWith(".gz") ? CompressionUtils.gzipInputStream(stream) : stream;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaticKodoFirehoseFactoryV2 that = (StaticKodoFirehoseFactoryV2) o;

    return Objects.equals(uris, that.uris) &&
           Objects.equals(prefixes, that.prefixes) &&
           getMaxCacheCapacityBytes() == that.getMaxCacheCapacityBytes() &&
           getMaxFetchCapacityBytes() == that.getMaxFetchCapacityBytes() &&
           getPrefetchTriggerBytes() == that.getPrefetchTriggerBytes() &&
           getFetchTimeout() == that.getFetchTimeout() &&
           getMaxFetchRetry() == that.getMaxFetchRetry();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        uris,
        prefixes,
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }
}
