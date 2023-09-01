import core
import sigs
import memstate
import memrk
import memd
import unittest
import hashlib
import nacl
from nacl import signing

class TestState(unittest.TestCase):
    def standard_object(self):
        fields = {
            "name": "object.txt",
            "format": "text/plain",
            "size": len("object contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"object contents").hexdigest(),
        }

        return core.Object(fields["name"], fields["format"], fields["size"], 
            fields["hash_type"], fields["hash"])

    def versioned_object(self, object_: core.Object):
        obj = object_.__copy__()
        obj.version += 1
        obj.name = "object.rtf"

        return obj

    def standard_schema(self):
        fields = {
            "name": "schema-sample",
            "format": "application/schema+json",
            "size": len("schema contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"schema contents").hexdigest(),
        }

        return core.Object(fields["name"], fields["format"], fields["size"], 
            fields["hash_type"], fields["hash"])

    def standard_annotation(self, schema: core.Identifier):
        fields = {
            "size": len("annotation contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"annotation contents").hexdigest(),
        }

        return core.Annotation(schema, fields["size"], fields["hash_type"], 
            fields["hash"])

    def versioned_annotation(self, annotation: core.Annotation):
        anno = annotation.__copy__()
        anno.version += 1

        return anno

    def test_machine_register(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        self.assertEqual(len(machine.validators), 1)
        self.assertEqual(machine.validators[0], state)
        self.assertEqual(len(machine.consumers), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.consumers[1], state)

    def test_object_create_accept(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 1)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            o1v0.identifier()])

        rae = core.ReviewAcceptEvent(oce.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 0)

    def test_object_update_accept(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 2)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertEqual(state.objects[o1v0.uuid][1], o1v1)
        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            o1v1.identifier()])

        rae = core.ReviewAcceptEvent(oue.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[o1v1.identifier()]), 0)

    def test_object_delete_accept(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 1)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertTrue(memstate.TagT.DELETE_PENDING in state.entity_status[
            o1v0.identifier()])

        rae = core.ReviewAcceptEvent(ode.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertTrue(memstate.TagT.DELETE_ACCEPTED in state.entity_status[
            o1v0.identifier()])
        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 2)

    def test_annotation_create_accept(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 1)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        
        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
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
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 2)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        self.assertEqual(state.annotations[a1v0.uuid][1], a1v1)

        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            a1v1.identifier()])

        rae = core.ReviewAcceptEvent(aue.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertEqual(len(state.entity_status[a1v1.identifier()]), 0)


    def test_annotation_delete_accept(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 1)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        self.assertTrue(memstate.TagT.DELETE_PENDING in state.entity_status[
            a1v0.identifier()])

        rae = core.ReviewAcceptEvent(ade.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        self.assertTrue(memstate.TagT.DELETE_ACCEPTED in state.entity_status[
            a1v0.identifier()])
        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 2)

    def test_owner_add(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer1 = sigs.Signer(sk1)
        
        vk1 = signer1.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)

        wae1 = signer1.sign(wae1)
        machine.process_event(wae1)

        self.assertEqual(len(state.owners), 1)

        sk2 = nacl.signing.SigningKey.generate()
        vk2 = sk2.verify_key
        oae2 = core.OwnerAddEvent(vk2)
        signer2 = sigs.Signer(sk2)

        with self.assertRaises(core.ValidationError):
            oae2 = signer2.sign(oae2)
            machine.process_event(oae2)    

        oae2 = signer1.sign(oae2)
        machine.process_event(oae2)

        self.assertEqual(len(state.owners), 2)

    def test_owner_remove(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertEqual(len(state.owners), 1)

        with self.assertRaises(core.ValidationError):
            ore2 = signer1.sign(core.OwnerRemoveEvent(vk1))
            machine.process_event(ore2)

    def test_object_create_reject(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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
        self.assertTrue(memstate.TagT.CREATE_REJECTED in state.entity_status[
            o1v0.identifier()])

    def test_object_update_reject(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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
        self.assertTrue(memstate.TagT.CREATE_REJECTED in state.entity_status[
            o1v1.identifier()])

    def test_object_delete_reject(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            o1v0.identifier()])
        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 1)

    def test_annotation_create_reject(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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
        self.assertTrue(memstate.TagT.CREATE_REJECTED in state.entity_status[
            a1v0.identifier()])

    def test_annotation_update_reject(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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
        self.assertTrue(memstate.TagT.CREATE_REJECTED in state.entity_status[
            a1v1.identifier()])

    def test_annotation_delete_reject(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

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

        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            a1v0.identifier()])
        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 1)

if __name__ == '__main__':
    unittest.main()