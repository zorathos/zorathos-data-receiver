package org.datacenter.agent.util;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.PersonnelAndPlanLoginConfig;
import org.datacenter.config.crew.PersonnelReceiverConfig;
import org.datacenter.config.plan.FlightPlanReceiverConfig;
import org.datacenter.config.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.model.plan.response.FlightPlanResponse;
import org.datacenter.receiver.util.MySQLDriverConnectionPool;
import org.datacenter.receiver.util.RetryUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 各个业务之间的Agent是独立的 但是util可以是一起的
 *
 * @author : [wangminan]
 * @description : 在本类中，我们调用HttpClient发送请求
 */
@Slf4j
@SuppressWarnings("deprecation")
public class PersonnelAndFlightPlanHttpClientUtil {
    private static final ObjectMapper mapper;
    private static final String redisKey = "human-machine:personnel-and-flight-plan:cookie";
    private static final Integer MAX_RETRY_COUNT = Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("agent.retries.http"));

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true);
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    /**
     * 登录人员与装备系统 一共有三条cookie 通过逗号分隔
     */
    public static void loginAndGetCookies(PersonnelAndPlanLoginConfig loginConfig) {
        String url = loginConfig.getLoginUrl();

        try (HttpClient client = HttpClient.newHttpClient()) {
            String cookies = RetryUtil.executeHttpRequestWithRetry(
                    () -> {
                        try {
                            return HttpRequest.newBuilder()
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(loginConfig.getLoginJson()))
                                    .uri(new URI(url))
                                    .build();
                        } catch (URISyntaxException e) {
                            throw new ZorathosException(e, "Failed to create cookie URL");
                        }
                    },
                    client,
                    MAX_RETRY_COUNT,
                    response -> response.headers()
                            .firstValue("Set-Cookie")
                            .orElseThrow(() -> new ZorathosException("Error occurs while login to personnel and flight plan system."))
            );
            // 需要上传这段Cookie到Redis
            RedisUtil.set(redisKey, cookies);
            log.info("Login to personnel and flight plan system successfully, current cached cookies: {}.", cookies);
        } catch (Throwable e) {
            throw new ZorathosException(e, "Encounter error while login to personnel and flight plan system.");
        }
    }

    public static List<FlightPlanRoot> getFlightRoots(FlightPlanReceiverConfig receiverConfig, MySQLDriverConnectionPool tidbFlightPlanPool) {
        log.info("Trying to get flight plans from sys api.");
        String formattedCookies = RedisUtil.get(redisKey);

        // 1. 整理未入库飞行日期
        List<FlightDate> flightDates;
        try (HttpClient client = HttpClient.newHttpClient()) {
            // 1.1 先从系统接口获取飞行日期列表
            String url = receiverConfig.getFlightDateUrl();
            log.info("Fetching flight dates from {}.", url);

            flightDates = RetryUtil.executeHttpRequestWithRetry(
                    () -> {
                        try {
                            return HttpRequest.newBuilder()
                                    .GET()
                                    .header("Cookie", formattedCookies)
                                    .header("Set-Cookie", formattedCookies)
                                    .uri(new URI(url))
                                    .build();
                        } catch (URISyntaxException e) {
                            throw new ZorathosException(e, "Failed to create flight date URL");
                        }
                    },
                    client,
                    MAX_RETRY_COUNT,
                    response -> {
                        if (response.statusCode() != 200) {
                            log.error("Error occurs while fetching flight date, response code: {}, response body: {}",
                                    response.statusCode(), response.body());
                            throw new ZorathosException("Error occurs while fetching flight dates.");
                        }
                        try {
                            return mapper.readValue(response.body(), mapper.getTypeFactory()
                                    .constructCollectionType(List.class, FlightDate.class));
                        } catch (Exception e) {
                            throw new ZorathosException(e, "Error parsing flight date response");
                        }
                    }
            );
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while fetching flight dates.");
        }

        // 1.2 从数据库拿已经有的飞行日期列表 把已经有的飞行日期从未入库飞行日期列表中移除
        List<LocalDate> flightDatesFromDB = getFlightDatesFromDB(tidbFlightPlanPool);
        flightDates.removeIf(flightDate -> flightDatesFromDB.contains(flightDate.getDate()));

        if (flightDates.isEmpty()) {
            // 直接返回空
            return new ArrayList<>();
        }

        log.info("Found {} flight dates remain to be captured.", flightDates.size());

        // 2. 拿飞行日期列表拼字段 获取 计划编号 和 飞行计划 列表
        List<FlightPlanRoot> flightPlans = new ArrayList<>();
        for (FlightDate flightDate : flightDates) {
            log.info("Fetching flight plan for date: {}.", flightDate.getDate());

            // 2.1. 从任务系统获取所有计划编号
            final String codeUrl = receiverConfig.getFlightCodeUrl() + flightDate.getDate().toString();
            PlanCode planCode;

            try (HttpClient client = HttpClient.newHttpClient()) {
                planCode = RetryUtil.executeHttpRequestWithRetry(
                        () -> {
                            try {
                                return HttpRequest.newBuilder()
                                        .GET()
                                        .header("Cookie", formattedCookies)
                                        .header("Set-Cookie", formattedCookies)
                                        .uri(new URI(codeUrl))
                                        .build();
                            } catch (URISyntaxException e) {
                                throw new ZorathosException(e, "Failed to create plan code URL");
                            }
                        },
                        client,
                        MAX_RETRY_COUNT,
                        response -> {
                            if (response.statusCode() != 200) {
                                log.error("Error occurs while fetching plan code, response code: {}, response body: {}",
                                        response.statusCode(), response.body());
                                throw new ZorathosException("Error occurs while fetching plan code.");
                            }
                            try {
                                return mapper.readValue(response.body(), PlanCode.class);
                            } catch (Exception e) {
                                throw new ZorathosException(e, "Error parsing plan code response");
                            }
                        }
                );
            } catch (Exception e) {
                throw new ZorathosException(e, "Error occurs while fetching mission codes.");
            }

            // 2.2 获取所有飞行计划
            try (HttpClient client = HttpClient.newHttpClient()) {
                log.info("Found plan code: {} for flight date: {}, fetching xml.", planCode.getCode(), flightDate.getDate());
                final String xmlUrl = receiverConfig.getFlightXmlUrl() + planCode.getCode();
                final LocalDate finalDate = flightDate.getDate();
                final String finalCode = planCode.getCode();

                FlightPlanRoot flightPlanRoot = RetryUtil.executeHttpRequestWithRetry(
                        () -> {
                            try {
                                return HttpRequest.newBuilder()
                                        .GET()
                                        .header("Cookie", formattedCookies)
                                        .header("Set-Cookie", formattedCookies)
                                        .uri(new URI(xmlUrl))
                                        .build();
                            } catch (URISyntaxException e) {
                                throw new ZorathosException(e, "Failed to create flight XML URL");
                            }
                        },
                        client,
                        MAX_RETRY_COUNT,
                        response -> {
                            if (response.statusCode() != 200) {
                                log.error("Error occurs while fetching flight plan, response code: {}, response body: {}",
                                        response.statusCode(), response.body());
                                throw new ZorathosException("Error occurs while fetching flight plan.");
                            }
                            try {
                                FlightPlanResponse planResponse = mapper.readValue(response.body(), FlightPlanResponse.class);
                                FlightPlanRoot root = FlightPlanRoot.fromXml(planResponse.getXml(), finalCode);
                                root.setFlightDate(finalDate);
                                return root;
                            } catch (Exception e) {
                                throw new ZorathosException(e, "Error parsing flight plan response");
                            }
                        }
                );

                flightPlans.add(flightPlanRoot);
            } catch (Exception e) {
                throw new ZorathosException(e, "Error occurs while fetching flight plans.");
            }
        }
        return flightPlans;
    }

    /**
     * 获取人员信息 因为没有做分页所以肯定是以一个完整列表的形式返回
     *
     * @param receiverConfig 接收器配置
     * @return 人员信息列表
     */
    public static List<PersonnelInfo> getPersonnelInfos(PersonnelReceiverConfig receiverConfig) {
        log.info("Trying to get personnel infos from sys api.");
        String formattedCookies = RedisUtil.get(redisKey);

        try (HttpClient client = HttpClient.newHttpClient()) {
            final String url = receiverConfig.getPersonnelUrl();

            return RetryUtil.executeHttpRequestWithRetry(
                    () -> {
                        try {
                            return HttpRequest.newBuilder()
                                    .GET()
                                    .header("Cookie", formattedCookies)
                                    .header("Set-Cookie", formattedCookies)
                                    .uri(new URI(url))
                                    .build();
                        } catch (URISyntaxException e) {
                            throw new ZorathosException(e, "Failed to create personnel URL");
                        }
                    },
                    client,
                    MAX_RETRY_COUNT,
                    response -> {
                        if (response.statusCode() != 200) {
                            log.error("Error occurs while fetching personnel infos, response code: {}, response body: {}",
                                    response.statusCode(), response.body());
                            throw new ZorathosException("Error occurs while fetching personnel infos.");
                        }
                        try {
                            return mapper.readValue(response.body(), mapper.getTypeFactory()
                                    .constructCollectionType(List.class, PersonnelInfo.class));
                        } catch (Exception e) {
                            throw new ZorathosException(e, "Error parsing personnel response");
                        }
                    }
            );
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while fetching personnel infos.");
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class FlightDate {
        /**
         * 飞行日期 FXRQ
         */
        @JsonProperty("FXRQ")
        @JsonFormat(pattern = "yyyy-MM-dd", timezone = "GMT+8")
        private LocalDate date;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class PlanCode {

        @JsonProperty("JHBH")
        private String code;
    }

    private static List<LocalDate> getFlightDatesFromDB(MySQLDriverConnectionPool tidbFlightPlanPool) {
        try {
            log.info("Fetching data from flight_plan_root.");
            Connection connection = tidbFlightPlanPool.getConnection();
            ResultSet resultSet = connection
                    .prepareStatement("SELECT flight_date FROM " + TiDBTable.FLIGHT_PLAN_ROOT.getName())
                    .executeQuery();
            List<LocalDate> flightDates = new ArrayList<>();
            while (resultSet.next()) {
                flightDates.add(resultSet.getDate("flight_date").toLocalDate());
            }
            return flightDates;
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while truncating personnel database.");
        }
    }
}
