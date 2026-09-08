package com.hanaame.twitterpic;

import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.widget.ScrollView;
import android.widget.TextView;
import androidx.appcompat.app.AppCompatActivity;
import java.net.InetAddress;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "TwitterPic";
    private TextView logView;
    private ScrollView scrollView;
    private Handler handler;
    private Runnable logUpdater;

    private native int nativeStartProxy(String bootstrapIP);
    private native void nativeStopProxy();
    private native int nativeGetProxyPort();
    private native String nativeGetLogs();

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

        // 加载 ECH 代理库
        try {
            System.loadLibrary("echproxy");
            appendLog("libechproxy.so loaded");
        } catch (UnsatisfiedLinkError e) {
            appendLog("ERROR: " + e.getMessage());
            return;
        }

        // 启动代理
        startProxy();

        // 每 500ms 更新日志
        logUpdater = () -> {
            String logs = nativeGetLogs();
            if (logs != null && !logs.equals(logView.getText().toString())) {
                logView.setText(logs);
                scrollView.post(() -> scrollView.fullScroll(ScrollView.FOCUS_DOWN));
            }
            handler.postDelayed(this::updateLogs, 500);
        };
        handler.postDelayed(this::updateLogs, 500);
    }

    private void startProxy() {
        appendLog("Resolving bootstrap IP...");
        String bootstrapIP = resolveBootstrapIP();
        appendLog("Bootstrap IP: " + (bootstrapIP != null ? bootstrapIP : "null"));

        int port = nativeGetProxyPort();
        if (port > 0) {
            appendLog("Proxy already running on port " + port);
            openBrowser(port);
            return;
        }

        appendLog("Starting proxy...");
        int newPort = nativeStartProxy(bootstrapIP);

        if (newPort > 0) {
            appendLog("Proxy started on port " + newPort);
            openBrowser(newPort);
        } else {
            appendLog("ERROR: Failed to start proxy");
        }
    }

    private void updateLogs() {
        if (isFinishing() || isDestroyed()) return;
        String logs = nativeGetLogs();
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
        String url = "http://127.0.0.1:" + port + "/";
        appendLog("Opening browser: " + url);
        try {
            Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
            startActivity(intent);
        } catch (Exception e) {
            appendLog("ERROR: " + e.getMessage());
        }
    }
}
