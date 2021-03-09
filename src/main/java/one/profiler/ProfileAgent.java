package one.profiler;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-05-21.
 */
public class ProfileAgent {
    /**
     * only support Oracle JDK and Sun JDK
     *
     */
    public void checkJvm() {
        String vmVendor = System.getProperty("java.vm.specification.vendor");
        boolean ret = (vmVendor != null &&
                ("Sun Microsystems Inc.".equals(vmVendor) || vmVendor.startsWith("Oracle")));
        if (!ret) {
            throw new RuntimeException("ProfileAgent only support Oracle JDK and Sun JDK");
        }
    }

    private static final String CONNECTOR_ADDRESS =
            "com.sun.management.jmxremote.localConnectorAddress";

    static Class<?> attachProviderCls;
    static Class<?> virtualMachineDescriptorCls;
    static Class<?> virtualMachineCls;

    /**
     * tools.jar are not always included used by default class loader, so we
     * will try to use custom loader that will try to load tools.jar
     *
     * @return
     */
    protected URLClassLoader initToolCls() throws MalformedURLException, ClassNotFoundException {
        String javaHome = System.getProperty("java.home");
        String tools = javaHome + File.separator +
                ".." + File.separator + "lib" + File.separator + "tools.jar";
        URLClassLoader loader = new URLClassLoader(new URL[]{new File(tools).toURI().toURL()});

        attachProviderCls = Class.forName("com.sun.tools.attach.spi.AttachProvider", true, loader);
        virtualMachineDescriptorCls = Class.forName("com.sun.tools.attach.VirtualMachineDescriptor", true, loader);
        virtualMachineCls = Class.forName("com.sun.tools.attach.VirtualMachine", true, loader);
        return loader;
    }

    public static String getContainerId() {
        String userDir = System.getProperty("user.dir");

        for (String name : userDir.split(File.separator)) {
            if (name.startsWith("container_")) {
                return name;
            }
        }
        throw new RuntimeException("没有找到container_id");
    }

    public static String pid;

    @SuppressWarnings("unchecked")
    public Object getDescriptor(Object attachProvider, String containerId) throws Exception {
        List<Object> virtualMachines = (List<Object>) attachProviderCls.getMethod("listVirtualMachines").invoke(attachProvider);
        Method displayNameMethod = virtualMachineDescriptorCls.getMethod("displayName");
        Method idMethod = virtualMachineDescriptorCls.getMethod("id");

        for (Object virtualMachine : virtualMachines) {
            String displayName = (String) displayNameMethod.invoke(virtualMachine);
            if (displayName.contains(containerId) && !displayName.contains("one.profiler.ProfileAgent")) {
                pid = (String) idMethod.invoke(virtualMachine);
                return virtualMachine;
            }
        }
        throw new RuntimeException("根据container_id没有找到对应的Java进程");
    }

    public String getConnectorAddress(Object virtualMachine) throws Exception {
        Properties agentProperties = (Properties) virtualMachineCls.getMethod("getAgentProperties").invoke(virtualMachine);
        String connectorAddress = agentProperties.getProperty(CONNECTOR_ADDRESS);
        if (connectorAddress == null) {
            Properties sysProperties = (Properties) virtualMachineCls.getMethod("getSystemProperties").invoke(virtualMachine);
            String agent = sysProperties.getProperty("java.home") + File.separator + "lib" + File.separator + "management-agent.jar";
            virtualMachineCls.getMethod("loadAgent", String.class).invoke(virtualMachine, agent);
            agentProperties = (Properties) virtualMachineCls.getMethod("getAgentProperties").invoke(virtualMachine);
            connectorAddress = agentProperties.getProperty(CONNECTOR_ADDRESS);
        }

        Enumeration<?> keys = agentProperties.propertyNames();
        System.out.println("agent properties:");
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            System.out.println(key.toString() + "\t" + agentProperties.get(key));
        }
        if (connectorAddress == null) {
            throw new RuntimeException("获取 connectorAddress 失败!");
        }
        virtualMachineCls.getMethod("detach").invoke(virtualMachine);
        return connectorAddress;
    }

    public AsyncProfilerMXBean getProfileProxy(String containerId) throws Exception {
        checkJvm();

        initToolCls();

        Object attachProvider = ((List) attachProviderCls.getMethod("providers", (Class[]) null)
                .invoke(null, (Object[]) null)).get(0);
        if (attachProvider == null) {
            throw new RuntimeException("获取 attachProvider 失败!");
        }

        Object descriptor = getDescriptor(attachProvider, containerId);
        if (descriptor == null) {
            throw new RuntimeException("获取 descriptor 失败!");
        }

        Object virtualMachine = attachProviderCls.getMethod("attachVirtualMachine", virtualMachineDescriptorCls)
                .invoke(attachProvider, descriptor);
        if (virtualMachine == null) {
            throw new RuntimeException("获取 virtualMachine 失败!");
        }

        String connectAddr = getConnectorAddress(virtualMachine);
        final JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectAddr));
        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();

        return JMX.newMBeanProxy(serverConnection,
                new ObjectName("one.profiler:type=AsyncProfiler"),
                AsyncProfilerMXBean.class);
    }

    public static void main(String[] args) throws Exception {
        String containerId;
        if (args.length >= 1) {
            containerId = args[0];
        } else {
            containerId = getContainerId();
        }

        ProfileAgent agent = new ProfileAgent();
        AsyncProfilerMXBean profiler = agent.getProfileProxy(containerId);

        Scanner scanner = new Scanner(System.in);
        String header = "*********************************************************\n" +
                " 请选择profile操作或其他profile命令，默认生成火焰图位置: /tmp/executor_{PID}.svg\n" +
                " 1. start profile\n" +
                " 2. stop profile\n" +
                " 3. exit\n";
        while (true) {
            System.out.println(header);
            String cmd = scanner.next();
            if (cmd.length() == 1) {
                Integer i = Integer.parseInt(cmd);
                if (i == 1) {
                    System.out.println("profile start.");
                    profiler.execute("start");
                } else if (i == 2) {
                    System.out.println("profile stop.");
                    profiler.execute("stop,file=/tmp/executor_" + pid + ".svg");
                } else if (i == 3) {
                    System.exit(0);
                } else {
                    System.out.println("参数输入错误");
                }
            } else {
                System.out.println("custom profile command :" + cmd);
                profiler.execute(cmd);
            }
        }
    }
}
