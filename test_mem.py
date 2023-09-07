import core
import sigs
import mem
import unittest
import hashlib
import nacl
from nacl import signing

class TestState(unittest.TestCase):
    def standard_object(self):
        return core.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def versioned_object(self, object_: core.Object):
        obj = object_.__copy__()
        obj.version += 1
        obj.name = "object.rtf"

        return obj

    def standard_schema(self):
        return core.Object(
            "schema-sample", 
            "application/schema+json", 
            len("schema contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"schema contents").hexdigest())

    def standard_annotation(self, schema: core.Identifier):
        return core.Annotation(
            schema, 
            len("annotation contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"annotation contents").hexdigest())

    def versioned_annotation(self, annotation: core.Annotation):
        anno = annotation.__copy__()
        anno.version += 1

        return anno

    def test_machine_register(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        self.assertEqual(len(machine.validators), 1)
        self.assertEqual(machine.validators[0], state_validator)
        self.assertEqual(len(machine.consumers), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.consumers[1], state_consumer)

    def test_object_create_accept(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        self.assertEqual(len(state.object_lookup), 1)
        self.assertEqual(len(state.object_lookup[o1v0.uuid]), 1)
        self.assertEqual(state.object_lookup[o1v0.uuid][0], o1v0)
        self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
            o1v0.identifier()])

        rae = core.ReviewAcceptEvent(oce.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 0)

    def test_object_update_accept(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        o1v1 = self.versioned_object(o1v0)
        oue = signer.sign(core.ObjectUpdateEvent(o1v1))
        machine.process_event(oue)

        self.assertEqual(len(state.object_lookup), 1)
        self.assertEqual(len(state.object_lookup[o1v0.uuid]), 2)
        self.assertEqual(state.object_lookup[o1v0.uuid][0], o1v0)
        self.assertEqual(state.object_lookup[o1v0.uuid][1], o1v1)
        self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
            o1v1.identifier()])

        rae = core.ReviewAcceptEvent(oue.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[o1v1.identifier()]), 0)

    def test_object_delete_accept(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        ode = signer.sign(core.ObjectDeleteEvent(o1v0.identifier()))
        machine.process_event(ode)

        self.assertEqual(len(state.object_lookup), 1)
        self.assertEqual(len(state.object_lookup[o1v0.uuid]), 1)
        self.assertEqual(state.object_lookup[o1v0.uuid][0], o1v0)
        self.assertTrue(core.StatusT.DELETE_PENDING in state.entity_status[
            o1v0.identifier()])

        rae = core.ReviewAcceptEvent(ode.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertTrue(core.StatusT.DELETE_ACCEPTED in state.entity_status[
            o1v0.identifier()])
        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 2)

    def test_annotation_create_accept(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))
        machine.process_event(sce)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        self.assertEqual(len(state.annotation_lookup), 1)
        self.assertEqual(len(state.annotation_lookup[a1v0.uuid]), 1)
        self.assertEqual(state.annotation_lookup[a1v0.uuid][0], a1v0)
        
        self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
            a1v0.identifier()])

        self.assertEqual(len(state.object_annotation_link.forward), 1)
        self.assertEqual(
            len(state.object_annotation_link.forward[o1v0.identifier()]), 1)
        self.assertEqual(state.object_annotation_link.forward[
            o1v0.identifier()][0], a1v0.uuid)

        self.assertEqual(len(state.object_annotation_link.reverse), 1)
        self.assertEqual(
            len(state.object_annotation_link.reverse[a1v0.uuid]), 1)
        self.assertEqual(state.object_annotation_link.reverse[a1v0.uuid][0], 
            o1v0.identifier())

        rae = core.ReviewAcceptEvent(ace.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 0)

    def test_annotation_update_accept(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))        
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))        
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
        machine.process_event(ace)

        a1v1 = self.versioned_annotation(a1v0)
        aue = signer.sign(core.AnnotationUpdateEvent(a1v1))        
        machine.process_event(aue)

        self.assertEqual(len(state.annotation_lookup), 1)
        self.assertEqual(len(state.annotation_lookup[a1v0.uuid]), 2)
        self.assertEqual(state.annotation_lookup[a1v0.uuid][0], a1v0)
        self.assertEqual(state.annotation_lookup[a1v0.uuid][1], a1v1)

        self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
            a1v1.identifier()])

        rae = core.ReviewAcceptEvent(aue.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[a1v1.identifier()]), 0)


    def test_annotation_delete_accept(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))            
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))            
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        ade = signer.sign(core.AnnotationDeleteEvent(a1v0.identifier()))    
        machine.process_event(ade)

        self.assertEqual(len(state.annotation_lookup), 1)
        self.assertEqual(len(state.annotation_lookup[a1v0.uuid]), 1)
        self.assertEqual(state.annotation_lookup[a1v0.uuid][0], a1v0)
        self.assertTrue(core.StatusT.DELETE_PENDING in state.entity_status[
            a1v0.identifier()])

        rae = core.ReviewAcceptEvent(ade.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertTrue(core.StatusT.DELETE_ACCEPTED in state.entity_status[
            a1v0.identifier()])
        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 2)

    def test_owner_add(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer1 = sigs.Signer(sk1)
        
        vk1 = signer1.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)

        wae1 = signer1.sign(wae1)
        machine.process_event(wae1)

        self.assertEqual(len(state.owner_list), 1)

        sk2 = nacl.signing.SigningKey.generate()
        signer2 = sigs.Signer(sk2)
        oae2 = core.OwnerAddEvent(signer2.verify_bytes)

        with self.assertRaises(core.ValidationError):
            oae2 = signer2.sign(oae2)
            machine.process_event(oae2)    

        oae2 = signer1.sign(oae2)
        machine.process_event(oae2)

        self.assertEqual(len(state.owner_list), 2)

    def test_owner_remove(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer1 = sigs.Signer(sk1)
        
        vk1 = signer1.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)

        wae1 = signer1.sign(wae1)
        machine.process_event(wae1)

        sk2 = nacl.signing.SigningKey.generate()
        signer2 = sigs.Signer(sk2)
        vk2 = signer2.verify_bytes
        oae2 = core.OwnerAddEvent(vk2)

        oae2 = signer1.sign(oae2)
        machine.process_event(oae2)

        with self.assertRaises(core.ValidationError):
            ore0 = signer2.sign(core.OwnerRemoveEvent(vk1))
            machine.process_event(ore0)

        ore1 = signer2.sign(core.OwnerRemoveEvent(vk2))
        machine.process_event(ore1)

        self.assertEqual(len(state.owner_list), 1)

        with self.assertRaises(core.ValidationError):
            ore2 = signer1.sign(core.OwnerRemoveEvent(vk1))
            machine.process_event(ore2)

    def test_object_create_reject(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        rre = core.ReviewRejectEvent(oce.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 1)
        self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
            o1v0.identifier()])

    def test_object_update_reject(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        o1v1 = self.versioned_object(o1v0)
        oue = signer.sign(core.ObjectUpdateEvent(o1v1))
        machine.process_event(oue)

        rre = core.ReviewRejectEvent(oue.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        self.assertEqual(len(state.entity_status[o1v1.identifier()]), 1)
        self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
            o1v1.identifier()])

    def test_object_delete_reject(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        ode = signer.sign(core.ObjectDeleteEvent(o1v0.identifier()))
        machine.process_event(ode)

        rre = core.ReviewRejectEvent(ode.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
            o1v0.identifier()])
        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 1)

    def test_annotation_create_reject(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))
        machine.process_event(sce)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        rre = core.ReviewRejectEvent(ace.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 1)
        self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
            a1v0.identifier()])

    def test_annotation_update_reject(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))        
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))        
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
        machine.process_event(ace)

        a1v1 = self.versioned_annotation(a1v0)
        aue = signer.sign(core.AnnotationUpdateEvent(a1v1))        
        machine.process_event(aue)

        rre = core.ReviewRejectEvent(aue.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        self.assertEqual(len(state.entity_status[a1v1.identifier()]), 1)
        self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
            a1v1.identifier()])

    def test_annotation_delete_reject(self):
        depot = mem.Depot()
        machine = core.Machine()

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = mem.StateConsumer(state)
        machine.register(state_consumer)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))            
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))            
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        ade = signer.sign(core.AnnotationDeleteEvent(a1v0.identifier()))    
        machine.process_event(ade)

        rre = core.ReviewRejectEvent(ade.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
            a1v0.identifier()])
        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 1)

if __name__ == '__main__':
    unittest.main()