package com.hanaame.twitterpic;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.widget.ScrollView;
import android.widget.TextView;
import androidx.appcompat.app.AppCompatActivity;
import java.net.InetAddress;

public class MainActivity extends AppCompatActivity {

    private TextView logView;
    private ScrollView scrollView;
    private Handler handler;

    // JNI 函数名必须与 Go //export 一致
    private native int StartProxy(String bootstrapIP);
    private native void StopProxy();
    private native int GetProxyPort();
    private native int IsEchReady();
    private native String GetLogs();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // 全屏日志界面
        scrollView = new ScrollView(this);
        logView = new TextView(this);
        logView.setPadding(16, 16, 16, 16);
        logView.setTextSize(12);
        logView.setFontFamily("monospace");
        scrollView.addView(logView);
        setContentView(scrollView);

        handler = new Handler(Looper.getMainLooper());

        // 加载 JNI 包装库（会自动加载 libechproxy.so）
        try {
            System.loadLibrary("jni-wrapper");
            appendLog("libjni-wrapper.so loaded");
        } catch (UnsatisfiedLinkError e) {
            appendLog("ERROR: " + e.getMessage());
            return;
        }

        // 启动代理
        startProxy();

        // 每 500ms 更新日志
        handler.postDelayed(this::updateLogs, 500);
    }

    private void startProxy() {
        appendLog("Resolving bootstrap IP...");
        String bootstrapIP = resolveBootstrapIP();
        appendLog("Bootstrap IP: " + (bootstrapIP != null ? bootstrapIP : "null"));

        int port = GetProxyPort();
        if (port > 0) {
            appendLog("Proxy already running on port " + port);
            openBrowser(port);
            return;
        }

        appendLog("Starting proxy...");
        int newPort = StartProxy(bootstrapIP);

        if (newPort > 0) {
            appendLog("Proxy started on port " + newPort);
            openBrowser(newPort);
        } else {
            appendLog("ERROR: Failed to start proxy");
        }
    }

    private void updateLogs() {
        if (isFinishing() || isDestroyed()) return;
        String logs = GetLogs();
        if (logs != null && !logs.equals(logView.getText().toString())) {
            logView.setText(logs);
            scrollView.post(() -> scrollView.fullScroll(ScrollView.FOCUS_DOWN));
        }
        handler.postDelayed(this::updateLogs, 500);
    }

    private void appendLog(String msg) {
        handler.post(() -> {
            logView.append(msg + "\n");
            scrollView.post(() -> scrollView.fullScroll(ScrollView.FOCUS_DOWN));
        });
    }

    private String resolveBootstrapIP() {
        try {
            InetAddress[] addresses = InetAddress.getAllByName("moonchan.xyz");
            for (InetAddress addr : addresses) {
                String host = addr.getHostAddress();
                if (host != null && !host.contains(":")) {
                    return host;
                }
            }
        } catch (Exception e) {
            appendLog("DNS resolution failed: " + e.getMessage());
        }
        return null;
    }

    private void openBrowser(int port) {
        // 使用 HTTPS（从 GitHub 下载证书）
        String url = "https://127.0.0.1:" + port + "/";
        appendLog("Opening browser: " + url);
        try {
            Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
            startActivity(intent);
        } catch (Exception e) {
            appendLog("ERROR: " + e.getMessage());
        }
    }
}
