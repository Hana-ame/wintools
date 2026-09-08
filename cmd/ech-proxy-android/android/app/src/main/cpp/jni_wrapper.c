// cmd/ech-proxy-android/android/app/src/main/cpp/jni_wrapper.c
// JNI 包装层：Java native 方法 → Go 导出函数
//
// Go 导出函数（//export）:
//   StartProxy(char* bootstrapIP) -> uint16
//   StopProxy() -> void
//   GetProxyPort() -> uint16
//   IsEchReady() -> int
//   GetLogs() -> char*
//
// JNI 函数名规则: Java_com_hanaame_twitterpic_MainActivity_<method>

#include <jni.h>
#include <stdlib.h>
#include <string.h>

// Go 导出函数声明（在 libechproxy.so 中）
extern uint16_t StartProxy(const char* bootstrapIP);
extern void StopProxy(void);
extern uint16_t GetProxyPort(void);
extern int IsEchReady(void);
extern char* GetLogs(void);

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL
Java_com_hanaame_twitterpic_MainActivity_StartProxy(
    JNIEnv *env,
    jobject thiz,
    jstring bootstrapIP) {

    const char *ipStr = NULL;
    if (bootstrapIP != NULL) {
        ipStr = (*env)->GetStringUTFChars(env, bootstrapIP, NULL);
    }

    uint16_t port = StartProxy(ipStr);

    if (ipStr != NULL) {
        (*env)->ReleaseStringUTFChars(env, bootstrapIP, ipStr);
    }

    return (jint)port;
}

JNIEXPORT void JNICALL
Java_com_hanaame_twitterpic_MainActivity_StopProxy(
    JNIEnv *env,
    jobject thiz) {

    StopProxy();
}

JNIEXPORT jint JNICALL
Java_com_hanaame_twitterpic_MainActivity_GetProxyPort(
    JNIEnv *env,
    jobject thiz) {

    uint16_t port = GetProxyPort();
    return (jint)port;
}

JNIEXPORT jint JNICALL
Java_com_hanaame_twitterpic_MainActivity_IsEchReady(
    JNIEnv *env,
    jobject thiz) {

    return (jint)IsEchReady();
}

JNIEXPORT jstring JNICALL
Java_com_hanaame_twitterpic_MainActivity_GetLogs(
    JNIEnv *env,
    jobject thiz) {

    char *logs = GetLogs();
    if (logs == NULL) {
        return NULL;
    }

    jstring result = (*env)->NewStringUTF(env, logs);
    free(logs);

    return result;
}

#ifdef __cplusplus
}
#endif
