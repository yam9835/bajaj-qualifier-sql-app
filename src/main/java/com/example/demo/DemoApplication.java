package com.example.demo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Map;

@SpringBootApplication
public class DemoApplication {

    private static final String NAME = "Guruvelli Yamini";
    private static final String REG_NO = "22BCE9835";
    private static final String EMAIL = "guruvelliyamini@gmail.com";

    private static final String BASE_URL = "https://bfhldevapigw.healthrx.co.in";

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    @Bean
    public CommandLineRunner run(RestTemplate restTemplate) {
        return args -> {
            System.out.println("=== Bajaj Qualifier Flow Started ===");

            // 1) Generate webhook
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> body = Map.of(
                    "name", NAME,
                    "regNo", REG_NO,
                    "email", EMAIL
            );

            HttpEntity<Map<String, String>> entity = new HttpEntity<>(body, headers);

            ResponseEntity<Map> response;
            try {
                response = restTemplate.postForEntity(
                        BASE_URL + "/hiring/generateWebhook/JAVA",
                        entity,
                        Map.class
                );
            } catch (Exception ex) {
                System.err.println("Error calling generateWebhook: " + ex.getMessage());
                ex.printStackTrace();
                return;
            }

            if (!response.getStatusCode().is2xxSuccessful()) {
                System.err.println("generateWebhook failed. Status: " + response.getStatusCode());
                return;
            }

            Map<String, Object> res = response.getBody();
            if (res == null) {
                System.err.println("generateWebhook returned empty body.");
                return;
            }

            Object webhookObj = res.get("webhook");
            Object tokenObj = res.get("accessToken");

            if (webhookObj == null || tokenObj == null) {
                System.err.println("Missing webhook or accessToken in response: " + res);
                return;
            }

            String webhook = webhookObj.toString();
            String token = tokenObj.toString();

            System.out.println("Webhook URL: " + webhook);
            System.out.println("Access Token: " + token);

            // 2) Your SQL Query (Question 1, because 35 is odd)
            String finalQuery =
                    "SELECT d.DEPARTMENT_NAME, t.total_amount AS SALARY, " +
                    "CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS EMPLOYEE_NAME, " +
                    "TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) AS AGE " +
                    "FROM ( " +
                    "  SELECT p.EMP_ID, SUM(p.AMOUNT) AS total_amount " +
                    "  FROM PAYMENTS p " +
                    "  WHERE DAY(p.PAYMENT_TIME) <> 1 " +
                    "  GROUP BY p.EMP_ID " +
                    ") t " +
                    "JOIN EMPLOYEE e ON e.EMP_ID = t.EMP_ID " +
                    "JOIN DEPARTMENT d ON d.DEPARTMENT_ID = e.DEPARTMENT " +
                    "JOIN ( " +
                    "  SELECT e2.DEPARTMENT, MAX(t2.total_amount) AS max_total " +
                    "  FROM ( " +
                    "    SELECT p2.EMP_ID, SUM(p2.AMOUNT) AS total_amount " +
                    "    FROM PAYMENTS p2 " +
                    "    WHERE DAY(p2.PAYMENT_TIME) <> 1 " +
                    "    GROUP BY p2.EMP_ID " +
                    "  ) t2 " +
                    "  JOIN EMPLOYEE e2 ON e2.EMP_ID = t2.EMP_ID " +
                    "  GROUP BY e2.DEPARTMENT " +
                    ") dm ON dm.DEPARTMENT = e.DEPARTMENT " +
                    "AND dm.max_total = t.total_amount " +
                    "ORDER BY d.DEPARTMENT_NAME;";

            System.out.println("Final SQL that will be submitted:");
            System.out.println(finalQuery);

            // 3) Submit solution
            HttpHeaders h2 = new HttpHeaders();
            h2.setContentType(MediaType.APPLICATION_JSON);
            // Problem statement: Authorization : <accessToken> (no "Bearer ")
            h2.set("Authorization", token);

            Map<String, String> payload = Collections.singletonMap("finalQuery", finalQuery);
            HttpEntity<Map<String, String>> entity2 = new HttpEntity<>(payload, h2);

            try {
                ResponseEntity<String> submitResponse =
                        restTemplate.postForEntity(webhook, entity2, String.class);

                System.out.println("Submit status: " + submitResponse.getStatusCode());
                System.out.println("Submit response body: " + submitResponse.getBody());
            } catch (Exception ex) {
                System.err.println("Error submitting finalQuery: " + ex.getMessage());
                ex.printStackTrace();
            }

            System.out.println("=== Bajaj Qualifier Flow Finished ===");
        };
    }
}
