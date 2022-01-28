import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:natbet/models/chat.dart';
import 'package:natbet/models/room.dart';
import 'package:natbet/services/toast.dart';

class RoomService {
  CollectionReference rooms = FirebaseFirestore.instance.collection('rooms');
  final roomsRef = FirebaseFirestore.instance
      .collection('rooms')
      .withConverter<RoomModel>(
        fromFirestore: (snapshot, _) => RoomModel.fromJson(snapshot.data()!),
        toFirestore: (movie, _) => movie.toJson(),
      );

  Future<void> createRoom(String roomName, String? password) {
    // Call the user's CollectionReference to add a new user
    if (password == '') {
      return rooms
          .add({
            'creator': FirebaseAuth.instance.currentUser!.uid,
            'roomName': roomName,
            'rid': DateTime.now().millisecondsSinceEpoch % 1000000,
            'members': [FirebaseAuth.instance.currentUser!.uid]
          })
          .then((value) => Toast().showToast("Create success"))
          .catchError(
              (error) => Toast().showToast("Failed to add user: $error"));
    } else {
      return rooms
          .add({
            'creator': FirebaseAuth.instance.currentUser!.uid,
            'roomName': roomName,
            'password': password,
            'rid': DateTime.now().millisecondsSinceEpoch % 1000000,
            'members': [FirebaseAuth.instance.currentUser!.uid]
          })
          .then((value) => Toast().showToast("Create success"))
          .catchError(
              (error) => Toast().showToast("Failed to add user: $error"));
    }
  }

  Stream<QuerySnapshot> getRooms() {
    return FirebaseFirestore.instance.collection('rooms').snapshots();
  }

  Stream getRoomById(String? rDocId) {
    return FirebaseFirestore.instance
        .collection('rooms')
        .doc(rDocId)
        .snapshots();
  }

  Future<void> createGame(
    String roomDocId,
    String gameType,
    String description,
    String leftValue,
    String rightValue,
    String condition,
  ) {
    CollectionReference game = FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection("games");
    return game
        .add({
          'winner': 'none',
          'gameType': gameType,
          'description': description,
          'creator': FirebaseAuth.instance.currentUser!.uid,
          'leftValue': leftValue,
          'leftBudget': 0,
          'rightValue': rightValue,
          'rightBudget': 0,
          'closed': false,
          'condition': condition,
          'closedInProgress': false,
          'refunded': false,
          'timestamp': Timestamp.now(),
        })
        .then((value) => Toast().showToast("Create success"))
        .catchError((error) => Toast().showToast("Failed to add user: $error"));
  }

  Future<void> updateGame(
    String roomDocId,
    String gameDocId,
    String winner,
  ) {
    DocumentReference game = FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection("games")
        .doc(gameDocId);
    return game
        .update({'winner': winner, 'closed': true})
        .then((value) => Toast().showToast("Update success"))
        .catchError((error) => Toast().showToast("Failed to add user: $error"));
  }

  Stream<QuerySnapshot> getGamesByRoom(roomDocId) {
    return FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection('games')
        .orderBy('timestamp', descending: false)
        .snapshots();
  }

  Stream<QuerySnapshot> getHistoriesByRoom(roomDocId) {
    return FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection('histories')
        .orderBy('timestamp', descending: true)
        .snapshots();
  }

  Future<void> playerBetting(
    String roomDocId,
    String gameDocId,
    var leftBudget,
    var rightBudget,
    int currentLeftUserBetValue,
    int currentRightUserBetValue,
  ) async {
    await FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection("games")
        .doc(gameDocId)
        .update({
      'leftBudget': leftBudget,
      'rightBudget': rightBudget,
    });

    await FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection("games")
        .doc(gameDocId)
        .collection('players')
        .doc(FirebaseAuth.instance.currentUser!.uid)
        .set({
      'leftBetBudget': currentLeftUserBetValue,
      'rightBetBudget': currentRightUserBetValue
    });
  }

  Stream getCurrentUserBetting(roomDocId, gameDocId) {
    return FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection('games')
        .doc(gameDocId)
        .collection('players')
        .doc(FirebaseAuth.instance.currentUser!.uid)
        .snapshots();
  }

  Stream<QuerySnapshot> getUsersBetting(roomDocId, gameDocId) {
    return FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection('games')
        .doc(gameDocId)
        .collection('players')
        .snapshots();
  }

  Stream<QuerySnapshot> getUsersBettingHistory(roomDocId, historyGameDocId) {
    return FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomDocId)
        .collection('histories')
        .doc(historyGameDocId)
        .collection('players')
        .snapshots();
  }

  Future<void> closeGame(var roomDocId, var gameDocId) {
    return rooms
        .doc(roomDocId)
        .collection('games')
        .doc(gameDocId)
        .update({'closed': true})
        .then((value) => Toast().showToast("Close success"))
        .catchError(
            (error) => Toast().showToast("Failed to close game: $error"));
  }

  Future<void> setCloseGameInProgress(var roomDocId, var gameDocId) {
    return rooms
        .doc(roomDocId)
        .collection('games')
        .doc(gameDocId)
        .update({'closedInProgress': true})
        .then((value) => Toast().showToast("Set close in progress success"))
        .catchError(
            (error) => Toast().showToast("Failed to close game: $error"));
  }

  Future<void> deleteGame(var roomDocId, var gameDocId) {
    return rooms
        .doc(roomDocId)
        .collection('games')
        .doc(gameDocId)
        .delete()
        .then((value) => Toast().showToast("Delete success"))
        .catchError(
            (error) => Toast().showToast("Failed to delete user: $error"));
  }

  final StreamController<List<Chat>> _chatController =
      StreamController.broadcast();
  final _allChatsResults = <Chat>[];
  int chatsLimit = 20;
  DocumentSnapshot? _lastDocument;
  bool _hasMoreMessages = true;
  bool _isQuerring = false;

  Stream listenToChatsRealTime(String roomId) {
    _requestMessages(roomId);
    return _chatController.stream;
  }

  void _requestMessages(String roomId) {
    var messagesQuery = FirebaseFirestore.instance
        .collection('rooms')
        .doc(roomId)
        .collection('chats')
        .orderBy('timestamp', descending: true)
        .limit(chatsLimit);
    //If we have a document start the query after it
    if (_lastDocument != null) {
      messagesQuery = messagesQuery.startAfterDocument(_lastDocument!);
    }

    if (!_hasMoreMessages) return;

    _isQuerring = true;
    messagesQuery.snapshots().listen((chatsSnapshot) {
      if (chatsSnapshot.docs.isNotEmpty) {
        var chats = chatsSnapshot.docs
            .map((snapshot) => Chat.fromMap(snapshot.data(), snapshot.id))
            .toList();
        if (_isQuerring) {
          _allChatsResults.addAll(chats);
          if (chatsSnapshot.docs.isNotEmpty) {
            _lastDocument = chatsSnapshot.docs.last;
          }
          _hasMoreMessages = chats.length == chatsLimit;
        }
        if (!_isQuerring) {
          _allChatsResults.insert(0, chats.first);
        }
        _chatController.add(_allChatsResults);
        _isQuerring = false;
      }
    });
  }

  Future sendChat(String content, String roomId) {
    return rooms.doc(roomId).collection('chats').add({
      'sender': FirebaseAuth.instance.currentUser!.uid,
      'content': content,
      'timestamp': Timestamp.now()
    });
  }
}
