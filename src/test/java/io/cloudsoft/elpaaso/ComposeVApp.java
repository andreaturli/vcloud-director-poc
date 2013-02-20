package io.cloudsoft.elpaaso;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.tryFind;
import static org.jclouds.vcloud.director.v1_5.VCloudDirectorLiveTestConstants.ENTITY_NON_NULL;
import static org.jclouds.vcloud.director.v1_5.VCloudDirectorLiveTestConstants.OBJ_FIELD_EQ;
import static org.jclouds.vcloud.director.v1_5.VCloudDirectorMediaType.VAPP;
import static org.jclouds.vcloud.director.v1_5.VCloudDirectorMediaType.VAPP_TEMPLATE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.URI;
import java.util.List;
import java.util.Set;

import org.jclouds.logging.Logger;
import org.jclouds.vcloud.director.v1_5.VCloudDirectorMediaType;
import org.jclouds.vcloud.director.v1_5.domain.AbstractVAppType;
import org.jclouds.vcloud.director.v1_5.domain.Reference;
import org.jclouds.vcloud.director.v1_5.domain.ResourceEntity.Status;
import org.jclouds.vcloud.director.v1_5.domain.Task;
import org.jclouds.vcloud.director.v1_5.domain.VApp;
import org.jclouds.vcloud.director.v1_5.domain.VAppTemplate;
import org.jclouds.vcloud.director.v1_5.domain.Vm;
import org.jclouds.vcloud.director.v1_5.domain.network.NatRule;
import org.jclouds.vcloud.director.v1_5.domain.network.NatService;
import org.jclouds.vcloud.director.v1_5.domain.network.Network;
import org.jclouds.vcloud.director.v1_5.domain.network.NetworkAssignment;
import org.jclouds.vcloud.director.v1_5.domain.network.NetworkConnection;
import org.jclouds.vcloud.director.v1_5.domain.network.NetworkConnection.IpAddressAllocationMode;
import org.jclouds.vcloud.director.v1_5.domain.network.NetworkServiceType;
import org.jclouds.vcloud.director.v1_5.domain.network.VAppNetworkConfiguration;
import org.jclouds.vcloud.director.v1_5.domain.params.ComposeVAppParams;
import org.jclouds.vcloud.director.v1_5.domain.params.InstantiationParams;
import org.jclouds.vcloud.director.v1_5.domain.params.SourcedCompositionItemParam;
import org.jclouds.vcloud.director.v1_5.domain.section.GuestCustomizationSection;
import org.jclouds.vcloud.director.v1_5.domain.section.NetworkConnectionSection;
import org.jclouds.vcloud.director.v1_5.features.VAppTemplateApi;
import org.jclouds.vcloud.director.v1_5.features.VmApi;
import org.jclouds.vcloud.director.v1_5.predicates.ReferencePredicates;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.base.CharMatcher;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class ComposeVApp extends BaseVCloudDirectorApiLiveTest {
    protected VmApi vmApi;
    protected VAppTemplateApi vAppTemplateApi;
    protected URI vAppUrn;
    protected Vm vm;
    protected VApp vApp;
    protected String vmUrn;

    @Override
    protected void setupEnvironment() {
        assertNotNull(context.getApi());
        vAppTemplateApi = context.getApi().getVAppTemplateApi();
        vmApi = context.getApi().getVmApi();

        // Get the configured Vdc for the tests
        vdc = lazyGetVdc();

        // Get the configured VAppTemplate for the tests
        vAppTemplate = vAppTemplateApi.get(vAppTemplateUrn);
        assertNotNull(vAppTemplate, String.format(ENTITY_NON_NULL, VAPP_TEMPLATE));
    }

    @Test
    public void testComposeVApp() {
        vApp = composeVApp();
        vAppUrn = vApp.getHref();
        vApp = vAppApi.get(vAppUrn); // refresh

        // Get the VMs
        List<Vm> vms = vApp.getChildren().getVms();
        vm = getOnlyElement(vms);
        vmUrn = vm.getId();
        assertFalse(vms.isEmpty(), "The VApp must have a Vm");
    }

    @Test(dependsOnMethods = "testComposeVApp")
    public void testPowerOnVApp() {
        Task powerOnVApp = vAppApi.powerOn(vAppUrn);
        assertTaskSucceedsLong(powerOnVApp);
        vApp = vAppApi.get(vAppUrn); // refresh
        assertVAppStatus(vApp, Status.POWERED_ON);
    }

    @Test(dependsOnMethods = "testPowerOnVApp")
    public void testSshToVMsInsideVApp() {
        Set<NatRule> natRules = extractNatMAppingFor(network);
        for (Vm child : vApp.getChildren().getVms()) {
            Set<NetworkConnection> networkConnections = vmApi.getNetworkConnectionSection(child.getHref())
                    .getNetworkConnections();
            assertTrue(size(filter(networkConnections, new ValidConnection(natRules, child))) > 0);
            tryFind(networkConnections, new ValidConnection(natRules, child)).get();
            // TODO computeService
            // compute.runScriptOnNodesMatching(inGroup(groupName),
            // exec(command))...
        }
    }

    @Test(dependsOnMethods = "testSshToVMsInsideVApp")
    public void testPowerOffVApp() {
        Task powerOffVApp = vAppApi.powerOff(vAppUrn);
        assertTaskSucceedsLong(powerOffVApp);
        vApp = vAppApi.get(vAppUrn); // refresh
        assertVAppStatus(vApp, Status.POWERED_OFF);
    }

    @AfterClass(description = "Cleans up the environment by deleting addd VApps")
    protected void cleanUpEnvironment() {
        vdc = vdcApi.get(vdcUrn); // Refresh

        // Find references in the Vdc with the VApp type and in the list of
        // instantiated VApp names
        Iterable<Reference> vApps = Iterables.filter(vdc.getResourceEntities(), Predicates.and(
                ReferencePredicates.<Reference> typeEquals(VCloudDirectorMediaType.VAPP),
                ReferencePredicates.<Reference> nameIn(vAppNames)));

        // If we found any references, remove the VApp they point to
        if (!Iterables.isEmpty(vApps)) {
            for (Reference ref : vApps) {
                cleanUpVApp(context.getApi().getVAppApi().get(ref.getHref()));
            }
        } else {
            logger.warn("No VApps in list found in Vdc %s (%s)", vdc.getName(), Iterables.toString(vAppNames));
        }
    }

    private VApp composeVApp() {
        Set<Vm> vms = getAvailableVMsFromVAppTemplate(vAppTemplate);
        // get the first vm to be added to vApp
        Vm toAddVm = Iterables.get(vms, 0);

        String name = name("composed-");
        VApp composedVApp = vdcApi.composeVApp(
                vdcUrn,
                ComposeVAppParams.builder().name(name)
                                           .sourcedItems(ImmutableList.of(sourcedItem(toAddVm)))
                                           .instantiationParams(instantiationParams())
                                           .build());
        Task creationTask = getFirst(composedVApp.getTasks(), null);
        if (creationTask == null) {
            fail("Can't compose a vApp");
        } else {
            assertTaskSucceedsLong(creationTask);
            // Save VApp name for cleanUp
            vAppNames.add(name);
        }
        return composedVApp;
    }

    private Set<NatRule> extractNatMAppingFor(Network network) {
        Set<NatRule> natRules = Sets.newLinkedHashSet();
        for (NetworkServiceType<?> networkServiceType : context.getApi().getNetworkApi().get(network.getHref())
                .getConfiguration().getNetworkFeatures().getNetworkServices()) {
            if (networkServiceType instanceof NatService) {
                natRules = ((NatService) networkServiceType).getNatRules();
            }
        }
        return natRules;
    }

    private SourcedCompositionItemParam sourcedItem(Vm toAddVm) {
        Reference reference = Reference.builder().name(name("vm-")).href(toAddVm.getHref()).type(toAddVm.getType())
                .build();
        SourcedCompositionItemParam vmItem = SourcedCompositionItemParam.builder().source(reference).build();
        if (vmHasNetworkConnectionConfigured(vm)) {
            // create a networkConnection to vApp network
            if (!vAppHasNetworkConfigured(vApp)) {
                InstantiationParams vmInstantiationParams = assignEmptyNetworkToVM();
                vmItem = SourcedCompositionItemParam.builder().fromSourcedCompositionItemParam(vmItem)
                        .instantiationParams(vmInstantiationParams).build();
            }

            // if the vm already contains a network connection section and if
            // the vapp contains a configured network
            // -> vm should be mapped to that network.
            else {
                Set<NetworkAssignment> networkAssignments = Sets.newLinkedHashSet();
                Set<VAppNetworkConfiguration> vAppNetworkConfigurations = listVappNetworkConfigurations(vApp);
                for (VAppNetworkConfiguration vAppNetworkConfiguration : vAppNetworkConfigurations) {
                    NetworkAssignment networkAssignment = NetworkAssignment.builder()
                            .innerNetwork(vAppNetworkConfiguration.getNetworkName())
                            .containerNetwork(vAppNetworkConfiguration.getNetworkName()).build();
                    networkAssignments.add(networkAssignment);
                    vmItem = SourcedCompositionItemParam.builder().fromSourcedCompositionItemParam(vmItem)
                            .networkAssignment(networkAssignments).build();
                }
            }
        } else {
            // check the type of networkConnection equals vApp network
            if (vAppHasNetworkConfigured(vApp)) {
                VAppNetworkConfiguration vAppNetworkConfiguration = getVAppNetworkConfig(vApp);
                NetworkConnection networkConnection = NetworkConnection.builder()
                        .network(vAppNetworkConfiguration.getNetworkName())
                        .ipAddressAllocationMode(IpAddressAllocationMode.DHCP).build();

                NetworkConnectionSection networkConnectionSection = NetworkConnectionSection.builder()
                        .info("networkInfo").primaryNetworkConnectionIndex(0).networkConnection(networkConnection)
                        .build();

                // adding the network connection section to the instantiation
                // params of the vapp.
                InstantiationParams vmInstantiationParams = InstantiationParams.builder()
                        .sections(ImmutableSet.of(networkConnectionSection)).build();
                vmItem = SourcedCompositionItemParam.builder().fromSourcedCompositionItemParam(vmItem)
                        .instantiationParams(vmInstantiationParams).build();
            } else {
                NetworkConnection networkConnection = NetworkConnection.builder().network(network.getName())
                        .isConnected(true).ipAddressAllocationMode(IpAddressAllocationMode.POOL).build();

                NetworkConnectionSection networkConnectionSection = NetworkConnectionSection.builder()
                        .info("networkInfo").primaryNetworkConnectionIndex(0).networkConnection(networkConnection)
                        .build();

                // adding the network connection section to the instantiation
                // params of the vapp.
                InstantiationParams vmInstantiationParams = InstantiationParams.builder()
                        .sections(ImmutableSet.of(networkConnectionSection)).build();
                vmItem = SourcedCompositionItemParam.builder().fromSourcedCompositionItemParam(vmItem)
                        .instantiationParams(vmInstantiationParams).build();
            }
        }
        return vmItem;
    }

    private InstantiationParams assignEmptyNetworkToVM() {
        // add a new network connection section for the vm.
        NetworkConnectionSection networkConnectionSection = NetworkConnectionSection.builder()
                .info("Empty network configuration parameters").build();
        // adding the network connection section to the instantiation params of
        // the vapp.
        return InstantiationParams.builder().sections(ImmutableSet.of(networkConnectionSection)).build();
    }

    private Set<Vm> getAvailableVMsFromVAppTemplate(VAppTemplate vAppTemplate) {
        return ImmutableSet.copyOf(Iterables.filter(vAppTemplate.getChildren(), new Predicate<Vm>() {
            // filter out vms in the vApp template with computer name that
            // contains underscores, dots,
            // or both.
            @Override
            public boolean apply(Vm input) {
                GuestCustomizationSection guestCustomizationSection = vmApi.getGuestCustomizationSection(input.getId());
                String computerName = guestCustomizationSection.getComputerName();
                String retainComputerName = CharMatcher.inRange('0', '9').or(CharMatcher.inRange('a', 'z'))
                        .or(CharMatcher.inRange('A', 'Z')).or(CharMatcher.is('-')).retainFrom(computerName);
                return computerName.equals(retainComputerName);
            }
        }));
    }

    private boolean vmHasNetworkConnectionConfigured(Vm vm) {
        for (NetworkConnection networkConnection : listNetworkConnections(vm)) {
            if (!networkConnection.getNetwork().equalsIgnoreCase("none")) {
                return true;
            }
        }
        return false;
    }

    private Set<NetworkConnection> listNetworkConnections(Vm vm) {
        if (vm == null)
            return Sets.newLinkedHashSet();
        return vmApi.getNetworkConnectionSection(vm.getHref()).getNetworkConnections();
    }

    private void assertVAppStatus(final VApp vApp, final Status status) {
        assertStatus(VAPP, vApp, status);
    }

    private static void assertStatus(final String type, final AbstractVAppType testVApp, final Status status) {
        assertEquals(testVApp.getStatus(), status,
                String.format(OBJ_FIELD_EQ, type, "status", status.toString(), testVApp.getStatus().toString()));
    }

    private class ValidConnection implements Predicate<NetworkConnection> {

        private final Set<NatRule> natRules;
        private final Vm child;

        private ValidConnection(Set<NatRule> natRules, Vm child) {
            this.natRules = checkNotNull(natRules, "natRules");
            this.child = checkNotNull(child, "child");
        }

        @Override
        public boolean apply(NetworkConnection networkConnection) {
            if (!Strings.nullToEmpty(networkConnection.getExternalIpAddress()).isEmpty()) {
                Logger.CONSOLE.info(String.format("vm(%s), externalIpAddress(%s)", child.getName(),
                        networkConnection.getExternalIpAddress()));
                return true;
            } else {
                if (natRules.isEmpty()) {
                    Logger.CONSOLE.error("Can't find a public IP address for vm(%s)", child.getName());
                    return false;
                }
                for (NatRule natRule : natRules) {
                    if (natRule.getOneToOneBasicRule().getInternalIpAddress()
                            .equalsIgnoreCase(networkConnection.getIpAddress())) {
                        Logger.CONSOLE.info(String.format(
                                "vm(%s), NAT Mapping -- internalIpAddress(%s) -> externalIpAddress(%s)", child
                                        .getName(), networkConnection.getIpAddress(), natRule.getOneToOneBasicRule()
                                        .getExternalIpAddress()));
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
