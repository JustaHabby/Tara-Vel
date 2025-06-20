package com.example.tara_velprototype

import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import com.google.firebase.Firebase
import com.google.firebase.auth.FirebaseAuth

class AuthViewModel : ViewModel() {
    private val auth: FirebaseAuth = FirebaseAuth.getInstance()

    private val _authState = MutableLiveData<AuthState>()
    val authState: LiveData<AuthState> = _authState

    init {
        checkAuthStatus()
    }

    fun checkAuthStatus() {

        if (auth.currentUser == null) {
            _authState.value = AuthState.Unauthenticated
        } else {
            _authState.value = AuthState.Authenticated
        }
    }

    fun login(email: String, password: String) {

        if (email.isEmpty() || password.isEmpty()) {
            _authState.value = AuthState.Error("Email or Password cant be Empty")
            return

        }
        _authState.value = AuthState.Loading
        auth.signInWithEmailAndPassword(email, password)
            .addOnCompleteListener { task ->
                if (task.isSuccessful) {
                    _authState.value = AuthState.Authenticated
                } else {
                    _authState.value = AuthState.Error(task.exception?.message ?: "Some ting wong")
                }
            }

    }

    fun signup(email: String, password: String) {

        if (email.isEmpty() || password.isEmpty()) {
            _authState.value = AuthState.Error("Email or Password cant be Empty")
            return

        }
        _authState.value = AuthState.Loading
        auth.createUserWithEmailAndPassword(email, password)
            .addOnCompleteListener { task ->
                if (task.isSuccessful) {
                    _authState.value = AuthState.Authenticated
                } else {
                    _authState.value = AuthState.Error(task.exception?.message ?: "Some ting wong")
                }
            }

    }

    fun signout() {
        auth.signOut()
        _authState.value = AuthState.Unauthenticated
    }

    fun updateEmailAndPassword(
        newEmail: String,
        newPassword: String,
        callback: (Boolean, String) -> Unit
    ) {
        val user = FirebaseAuth.getInstance().currentUser
        if (user != null) {
            user.updateEmail(newEmail).addOnCompleteListener { emailTask ->
                if (emailTask.isSuccessful) {
                    user.updatePassword(newPassword).addOnCompleteListener { passTask ->
                        if (passTask.isSuccessful) {
                            callback(true, "Credentials updated successfully.")
                        } else {
                            callback(
                                false,
                                passTask.exception?.message ?: "Failed to update password."
                            )
                        }
                    }
                } else {
                    callback(false, emailTask.exception?.message ?: "Failed to update email.")
                }
            }
        } else {
            callback(false, "No user logged in.")
        }
    }


    sealed class AuthState {
        object Authenticated : AuthState()
        object Unauthenticated : AuthState()
        object Loading : AuthState()
        data class Error(val message: String) : AuthState()


    }
}