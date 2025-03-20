package org.datacenter.agent.util;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.personnel.PersonnelReceiverConfig;
import org.datacenter.config.plan.FlightPlanReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * 各个业务之间的Agent是独立的 但是util可以是一起的
 *
 * @author : [wangminan]
 * @description : 在本类中，我们调用HttpClient发送请求
 */
@Slf4j
public class PersonnelAndFlightPlanHttpClientUtil {
    private static final ObjectMapper mapper;

    private static String localCookiesCache;

    static {
        mapper = new ObjectMapper();
    }

    private static final String host = humanMachineProperties.getProperty("agent.personnelAndFlightPlan.host");

    static {
        loginAndGetCookies();
    }

    /**
     * 登录人员与装备系统 一共有三条cookie 通过逗号分隔
     */
    private static void loginAndGetCookies() {
        String url = host + "/home/login";
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder()
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            "{" +
                                    "userInput:\"" + humanMachineProperties.getProperty("agent.personnelAndFlightPlan.login.username") + "\"," +
                                    "grbsInput:\"" + humanMachineProperties.getProperty("agent.personnelAndFlightPlan.login.username") + "\"," +
                                    "passwordInput:\"" + humanMachineProperties.getProperty("agent.personnelAndFlightPlan.login.password") + "\"" +
                                    "}"
                    ))
                    .uri(new URI(url))
                    .build();
            // 同步的请求
            localCookiesCache = client.send(request, HttpResponse.BodyHandlers.ofString())
                    .headers()
                    .allValues("Set-Cookie")
                    .stream()
                    // 加上"; "结尾
                    .map(cookie -> cookie + "; ")
                    .reduce(String::concat)
                    .orElseThrow();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new ZorathosException(e, "Encounter error while login to personnel and flight plan system.");
        }
    }

    public static List<FlightPlanRoot> getFlightRoots(FlightPlanReceiverConfig receiverConfig) {
        log.info("Trying to get flight plans from sys api.");
        String formattedCookies = localCookiesCache;
        // 获取今天日期 以yyyy-MM-dd输出
        String today = LocalDate.now().toString();

        // 1. 整理未入库飞行日期
        List<FlightDate> flightDates;
        try (HttpClient client = HttpClient.newHttpClient()) {
            // 1.1 先从系统接口获取飞行日期列表
            String url = host + "/fxjh/getfxrq?from=1970-01-01&to=" + today + "&dwdm=90121";
            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .header("Cookie", formattedCookies)
                    .header("Set-Cookie", formattedCookies)
                    .uri(new URI(url))
                    .build();
            // 获取响应
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            // 使用objectMapper 序列化返回列表
            flightDates = mapper.readValue(response.body(), mapper.getTypeFactory()
                    .constructCollectionType(List.class, FlightDate.class));
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new ZorathosException(e, "Error occurs while fetching flight dates.");
        }

        // 1.2 从数据库拿已经有的飞行日期列表 把已经有的飞行日期从未入库飞行日期列表中移除
        List<LocalDate> flightDatesFromDB = getFlightDatesFromDB();
        flightDates.removeIf(flightDate -> flightDatesFromDB.contains(flightDate.getDate()));

        // 2. TODO 从任务系统获取所有任务编号 任务编号要走别的系统 等现场调试
        Set<String> missionCodes = new HashSet<>();
        missionCodes.add("60225");
        Set<String> presetMissionCodes = receiverConfig.getQueryCodes();
        missionCodes.addAll(presetMissionCodes);

        // 3. 拿飞行日期列表拼字段 获取飞行计划列表
        List<FlightPlanRoot> flightPlans = new ArrayList<>();
        for (String missionCode : missionCodes) {
            for (FlightDate flightDate : flightDates) {
                // 把FlightDate转换成"yyyyMMdd"类型的字符串
                String date = flightDate.getDate().toString().replace("-", "");
                try (HttpClient client = HttpClient.newHttpClient()) {
                    // 90121是个常量
                    String url = host + "/fxdt/getxml?jhbh=" + date + "-90121-" + missionCode;
                    HttpRequest request = HttpRequest.newBuilder()
                            .GET()
                            .header("Cookie", formattedCookies)
                            .header("Set-Cookie", formattedCookies)
                            .uri(new URI(url))
                            .build();
                    // 获取响应
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    // 响应体为一段XML
                    String xml = response.body();
                    // 解析XML文件
                    FlightPlanRoot flightPlanRoot = FlightPlanRoot.fromXml(xml);
                    // 追加日期
                    flightPlanRoot.setFlightDate(flightDate.getDate());
                    flightPlans.add(flightPlanRoot);
                } catch (URISyntaxException | IOException | InterruptedException e) {
                    throw new ZorathosException(e, "Error occurs while fetching flight plans.");
                }
            }
        }
        return flightPlans;
    }

    public static List<PersonnelInfo> getPersonnelInfos(PersonnelReceiverConfig receiverConfig) {
        log.info("Trying to get personnel infos from sys api.");
        String formattedCookies = localCookiesCache;
        List<PersonnelInfo> personnelInfos;
        try (HttpClient client = HttpClient.newHttpClient()) {
            String url = host + "/fxy/bindfxylb?dwdm=90121" +
                    receiverConfig.getQueryString();
            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .header("Cookie", formattedCookies)
                    .header("Set-Cookie", formattedCookies)
                    .uri(new URI(url))
                    .build();
            // 获取响应
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            // 使用objectMapper 序列化返回列表
            personnelInfos = mapper.readValue(response.body(), mapper.getTypeFactory()
                    .constructCollectionType(List.class, PersonnelInfo.class));
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new ZorathosException(e, "Error occurs while fetching personnel infos.");
        }
        return personnelInfos;
    }

    @Data
    public static class FlightDate {
        /**
         * 飞行日期 FXRQ
         */
        @JsonProperty("FXRQ")
        @JsonFormat(pattern = "yyyy-MM-dd", timezone = "GMT+8")
        private LocalDate date;
    }

    private static List<LocalDate> getFlightDatesFromDB() {
        try {
            log.info("Fetching data from flight_plan_root.");
            Class.forName(humanMachineProperties.getProperty("tidb.driverName"));
            Connection connection = DriverManager.getConnection(
                    JdbcSinkUtil.TIDB_URL_FLIGHT_PLAN,
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
            ResultSet resultSet = connection.prepareStatement("select * from " +
                            TiDBDatabase.FLIGHT_PLAN.getName() +
                            TiDBTable.FLIGHT_PLAN_ROOT.getName())
                    .executeQuery();
            List<LocalDate> flightDates = new ArrayList<>();
            while (resultSet.next()) {
                flightDates.add(resultSet.getDate("flight_date").toLocalDate());
            }
            return flightDates;
        } catch (SQLException | ClassNotFoundException e) {
            throw new ZorathosException(e, "Error occurs while truncating personnel database.");
        }
    }
}
