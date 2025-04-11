package org.datacenter.agent.util;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * @author : [wangminan]
 * @description : MinIO工具类 用来封装一些常用的方法
 */
@Slf4j
public class MinioUtil {
    private static final MinioClient minioClient;

    static {
        minioClient = MinioClient.builder()
                .endpoint(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.endpoint"))
                .credentials(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.accessKeyId"), HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.accessKeySecret"))
                .build();
        // 检查 bucket是否存在 如果不存在的就创建
        try {
            if (!minioClient.bucketExists(BucketExistsArgs.builder()
                    .bucket(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"))
                    .build())) {
                log.info("Human machine bucket does not exist, creating:{}", HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"));
                minioClient.makeBucket(MakeBucketArgs.builder()
                        .bucket(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"))
                        .build());
            }
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter an error while initializing MinIO bucket");
        }
    }

    public static boolean checkFileExist(String url) {
        try {
            StatObjectResponse statObjectResponse = minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"))
                            .object(url)
                            .build());
            if (statObjectResponse != null) {
                return true;
            }
        } catch (ServerException | InsufficientDataException | ErrorResponseException | IOException |
                 NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException | XmlParserException |
                 InternalException e) {
            if (e instanceof ErrorResponseException) {
                if (((ErrorResponseException) e).response().code() == 404) {
                    return false;
                }
            } else {
                throw new ZorathosException(e, "Encounter an error while checking file existence");
            }
        }
        return false;
    }

    /**
     * 上传文件
     *
     * @param url         文件路径
     * @param inputStream 输入流
     */
    public static void upload(String url, InputStream inputStream) {
        try {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"))
                            .object(url)
                            .stream(
                                    inputStream, -1, 10485760)
                            .contentType("application/octet-stream")
                            .build());
        } catch (ServerException | InsufficientDataException | ErrorResponseException | IOException |
                 NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException | XmlParserException |
                 InternalException e) {
            throw new ZorathosException(e, "Encounter an error while uploading file: " + url);
        }
    }

    /**
     * 这里做比较基础的两种就行了 其他应该靠前端直连MinIO来做
     *
     * @param url         文件路径
     * @param file        文件
     * @param contentType 文件类型
     */
    public static void upload(String url, File file, String contentType) {
        try {
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"))
                            .object(url)
                            .filename(file.getName())
                            .contentType(contentType)
                            .build());
        } catch (ServerException | InsufficientDataException | ErrorResponseException | IOException |
                 NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException | XmlParserException |
                 InternalException e) {
            throw new ZorathosException(e, "Encounter an error while uploading file: " + file.getName());
        }
    }

    public static void delete(String url) {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(HumanMachineSysConfig.getHumanMachineProperties().getProperty("minio.bucket"))
                    .object(url)
                    .build());
        } catch (ServerException | InsufficientDataException | ErrorResponseException | IOException |
                 NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException | XmlParserException |
                 InternalException e) {
            throw new ZorathosException(e, "Encounter an error while deleting file: " + url);
        }
    }
}
