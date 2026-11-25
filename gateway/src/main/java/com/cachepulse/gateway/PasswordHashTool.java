package com.cachepulse.gateway;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 生成管理员密码的 BCrypt 哈希，填到 GATEWAY_ADMIN_PASSWORD_HASH。密码从控制台读，不出现在命令行和历史记录里：
 * <pre>
 * java "-Dloader.main=com.cachepulse.gateway.PasswordHashTool" -cp gateway/target/gateway-0.1.0-SNAPSHOT.jar org.springframework.boot.loader.launch.PropertiesLauncher
 * </pre>
 */
public final class PasswordHashTool {
    private PasswordHashTool() { }

    public static void main(String[] args) throws Exception {
        Console console = System.console();
        String password;
        if (console != null) {
            char[] typed = console.readPassword("Admin password: ");
            password = typed == null ? "" : new String(typed);
        } else {
            System.err.print("Admin password: ");
            password = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)).readLine();
        }
        if (password == null || password.length() < 12) {
            System.err.println("Password must have at least 12 characters");
            System.exit(1);
        }
        System.out.println(new BCryptPasswordEncoder(12).encode(password));
    }
}
