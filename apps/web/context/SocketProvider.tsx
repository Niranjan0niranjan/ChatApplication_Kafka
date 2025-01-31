"use client";
import React, { useCallback, useContext, useEffect, useState } from "react";
import { io, Socket } from "socket.io-client";

// Define the props for the SocketProvider component
interface SocketProviderProps {
  children?: React.ReactNode;
}

// Define the context interface for socket operations
interface ISocketContext {
  sendMessage: (msg: string) => any;
  messages: string[];
}

// Create the Socket context with an initial value of null
const SocketContext = React.createContext<ISocketContext | null>(null);

// Custom hook to use the Socket context
export const useSocket = () => {
  const state = useContext(SocketContext);
  if (!state) throw new Error(`state is undefined`);

  return state;
};

// SocketProvider component to provide socket functionality to children components
export const SocketProvider: React.FC<SocketProviderProps> = ({ children }) => {
  const [socket, setSocket] = useState<Socket>();
  const [messages, setMessages] = useState<string[]>([]);

  // Function to send a message through the socket
  const sendMessage: ISocketContext["sendMessage"] = useCallback(
    (msg) => {
      console.log("Send Message", msg);
      if (socket) {
        socket.emit("event:message", { message: msg });
      }
    },
    [socket]
  );

  // Callback function to handle received messages from the server
  const onMessageRec = useCallback((msg: string) => {
    console.log("From Server Msg Rec", msg);
    const { message } = JSON.parse(msg) as { message: string };
    setMessages((prev) => [...prev, message]);
  }, []);

  // Effect to initialize the socket connection and set up event listeners
  useEffect(() => {
    const _socket = io("http://localhost:8000");
    _socket.on("message", onMessageRec);

    setSocket(_socket);

    return () => {
      _socket.off("message", onMessageRec);
      _socket.disconnect();
      setSocket(undefined);
    };
  }, [onMessageRec]);

  // Provide the socket context to children components
  return (
    <SocketContext.Provider value={{ sendMessage, messages }}>
      {children}
    </SocketContext.Provider>
  );
};
